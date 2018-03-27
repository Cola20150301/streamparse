"""
Run a local Storm topology.

Note: If you have "org.apache.storm" in your uberjar-exclusions list in your
project.clj, this will fail. Temporarily remove it to use `sparse run`. You will
also need to add [org.apache.storm/flux-core "1.0.1"] to dependencies.
"""

from __future__ import absolute_import, print_function

from argparse import RawDescriptionHelpFormatter
from collections import defaultdict
from io import BytesIO
from tempfile import NamedTemporaryFile

from fabric.api import local, show
from ruamel import yaml

from ..storm import BatchingBolt, Spout, TicklessBatchingBolt
from ..util import (get_config, get_env_config, get_topology_definition,
                    get_topology_from_file, local_storm_version,
                    set_topology_serializer, storm_lib_version)
from .common import (add_ackers, add_config, add_debug, add_environment, add_name,
                     add_options, add_workers, resolve_options)
from .jar import jar_for_deploy


def run_local_topology(name=None, env_name=None, time=0, options=None, config_file=None):
    """Run a topology locally using Flux and `storm jar`."""
    name, topology_file = get_topology_definition(name, config_file=config_file)
    config = get_config(config_file=config_file)
    env_name, env_config = get_env_config(env_name, config_file=config_file)
    topology_class = get_topology_from_file(topology_file)

    set_topology_serializer(env_config, config, topology_class)
    serializer = env_config.get('serializer', config.get('serializer', 'json'))

    storm_options = resolve_options(options, env_config, topology_class, name,
                                    local_only=True)
    if storm_options['topology.acker.executors'] != 0:
        storm_options['topology.acker.executors'] = 1
    storm_options['topology.workers'] = 1
    storm_options['topology.name'] = name


    inputs = {}
    outputs = {}
    components = {}
    spouts = {}
    bolts = {}

    # Set parallelism based on env_name if necessary
    for spec in topology_class.specs:
        # TODO: Remove this unless we start actually using par
        if isinstance(spec.par, dict):
            spec.par = spec.par.get(env_name)
        # create a single instance of the class represented by each spec
        if issubclass(spec.component_cls, TicklessBatchingBolt):
            # Remove TicklessBatchingBolt from __mro__ to simplify life
            spec.component_cls = type(spec.component_cls.__name__,
                                      (BatchingBolt,),
                                      dict(vars(spec.component_cls)))
            # convert secs_between_batches to ticks_between_batches
            spec.component_cls.ticks_between_batches = spec.component_cls.secs_between_batches
            storm_options['topology.tick.tuple.freq.secs'] = 1

        component = spec.component_cls(input_stream=BytesIO(),
                                       output_stream=BytesIO(),
                                       serializer=serializer)
        components[spec.name] = component

        if isinstance(component, Spout):
            spouts[spec.name] = component
        else:
            bolts[spec.name] = component

        # Save streams for later for easier access
        inputs[spec.name] = spec.inputs
        outputs[spec.name] = spec.outputs

    # Initialize all components
    for task_id, (component_name, component) in enumerate(components.items()):
        context = {'topology.name': name,
                   'taskid': task_id,
                   'componentid': component_name}
        sources = defaultdict(dict)
        for stream_id, grouping in inputs[component_name].items():
            input_component = stream_id.componentId
            input_stream = stream_id.streamId
            input_fields = outputs[input_component][input_stream].output_fields
            sources[input_component][input_stream] = input_fields
        context['source'] = sources
        component._setup_component(storm_options, context)
        component.initialize(storm_options, context)

    # Loop through spouts calling next_tuple()
    for spout_name, spout_obj in spouts.items():
        # Create next_tuple command
        command = spout_obj.serializer.serialize_dict({'command': 'next'})
        spout_obj.serializer.input_stream.truncate(0)
        spout_obj.serializer.input_stream.write(command)
        spout_obj.serializer.input_stream.seek(0)
        # Read command and call process
        spout_obj._run()
        # Deserialize output
        import ipdb; ipdb.set_trace()
        # Send output to appropriate downstream bolts



def subparser_hook(subparsers):
    """ Hook to add subparser for this command. """
    subparser = subparsers.add_parser('run_pure',
                                      description=__doc__,
                                      help=main.__doc__,
                                      formatter_class=RawDescriptionHelpFormatter)
    subparser.set_defaults(func=main)
    add_ackers(subparser)
    add_config(subparser)
    add_debug(subparser)
    add_environment(subparser)
    add_name(subparser)
    add_options(subparser)
    subparser.add_argument('-t', '--time',
                           default=0,
                           type=int,
                           help='Time (in seconds) to keep local cluster '
                                'running. If time <= 0, run indefinitely. '
                                '(default: %(default)s)')
    add_workers(subparser)


def main(args):
    """Run the local topology with the given arguments"""
    run_local_topology(name=args.name, time=args.time, options=args.options,
                       env_name=args.environment, config_file=args.config)
