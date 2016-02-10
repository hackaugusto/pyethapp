# -*- coding: utf8 -*-
'''
This module implements some profiling strategies.

By default if the module is installed in the DATA_ROOT/contrib/ folder it will
profile the memory and the objcount of the running application, with the use of
signals, `kill -USR2 $(pgrep pyethapp)` for instance, other strategies can be
enabled. Since one strategy can interfere with one another only one of these
strategies will run at a given time.

There are two aditional strategies, cpu profiling and memory tracing, to enable
profiling use the SIGUSR1 while tracing SIGUSR2.

All the profiling data is save in the DATA_ROOT and can be used to analyse the
application, for memory time-line with memory usage and number of blocks
through time you can use the following command:

    python -m profiling memory timeline DATA_ROOT/*_memory.data

For the latency of sampling, you can use the following plot:

    python -m profiling latency scatter DATA_ROOT/*_memory.data

For visualizing the most common classes by count:

    python -m profiling memory objcount DATA_ROOT/*_objects.pickle

Flame graphs can be generated with Brendan Gregg's tool brendangregg/FlameGraph:

    cat DATA_ROOT/*_stack.data | flamegraph > flamegraph.svg

Sampling profiling will usually produce data for the same points (with the
exception of the objcount that is more coarse), so these plots can be combined
using ImageMagick:

    convert -append memory_timeline.png latency_scatter.png memory_objcount.png collage.png
'''
#
# Improvements:
#
# - Add environment variables to configure behavior:
#   - directory for saving profilling data
#   - starting profiling
#   - sampling interval
# - The objcount itself is not that uself, add the _sys.getsizeof_ to know the
#   amount of memory used by the type (heapy is a good alternative for this)
# - Draw the composite graphs with matplotlib instead of using convert and make
#   sure to aling all the points
# - Experiment with heapy or PySizer for memory profiling / leak hunting
# - Add the option to filter the data for plotting, effectivelly allowing
#   zooming
# - The startup cannot be profiled because of the order of importing, figure a
#   way to profile as much as possible
#
import atexit
import collections
import gc
import os
import pickle
import signal
import time
from datetime import datetime, timedelta
from itertools import chain

import gevent
import GreenletProfiler
import objgraph
import psutil

from ethereum import slogging

log = slogging.get_logger('profiling')
INTERVAL_SECONDS = 1.0
MEGA = float(2 ** 20)
SECOND = 1
MINUTE = 60 * SECOND
# TIMER_SIGNAL = signal.SIGALRM
# TIMER =  signal.ITIMER_REAL
# TIMER_SIGNAL = signal.SIGVTALRM
# TIMER =  signal.ITIMER_VIRTUAL
TIMER_SIGNAL = signal.SIGPROF
TIMER =  signal.ITIMER_PROF

# column position in the data file
TIMESTAMP = 0
BLOCKS = 1
MEMORY = 2

START = datetime.now()

# GLOBALS
# these are for the on_block() sampling
_block_number = None
_memory100_stream = None

# these are used for deterministic profiling (need to turn on with a signal)
_cpu_profiling = None

# these are used for object counting and memory allocation measurements
_trace_profiling = False
_trace_stream = None

# these are used for sampling profiling (on by default)
_sample_profiling = False
_memory_stream = None
_objects_stream = None
_stack_stream = None
_stack_count = None
_last_sample = 0  # this value will force the first call of sample() to do a sample_objects


class Timer(object):
    def __init__(self, callback, timer=TIMER, interval=INTERVAL_SECONDS, timer_signal=TIMER_SIGNAL):
        assert callable(callback), 'callback must be callable'

        signal.signal(timer_signal, self.callback)
        signal.setitimer(timer, interval, interval)

        oldtimer, oldaction = None, None  # cheating for now
        self.oldaction = oldaction
        self.oldtimer = oldtimer
        self.callback = callback

    def callback(self, signum, stack):
        self.callback(signum, stack)

        if self.oldaction and callable(self.oldaction):
            self.oldaction(signum, stack)

    def __del__(self):
        self.callback = None

        if self.oldaction and callable(self.oldaction):
            signal.signal(TIMER_SIGNAL, oldaction)
            signal.setitimer(TIMER_SIGNAL, self.oldtimer[0], self.oldtimer[1])
        else:
            signal.signal(TIMER_SIGNAL, signal.SIG_IGN)

    def __nonzero__(self):
        # we're always truthy
        return True


def ts_to_dt(string_ts):
    '''converts a string timestamp to a datatime object'''
    return datetime.fromtimestamp(int(float(string_ts)))


def frame_format(frame):
    block_name = frame.f_code.co_name
    module_name = frame.f_globals.get('__name__')
    return '{}({})'.format(block_name, module_name)


def flamegraph_format(stack_count):
    return '\n'.join('%s %d' % (key, value) for key, value in sorted(stack_count.items()))


def process_memory_mb(pid):
    process = psutil.Process(os.getpid())
    memory = process.memory_info()[0]
    for child in process.children(recursive=True):
        memory += child.memory_info()[0]
    return memory / MEGA


def sample_stack(stack_count, frame):
    callstack = []
    while frame is not None:
        callstack.append(frame_format(frame))
        frame = frame.f_back

    formatted_stack = ';'.join(reversed(callstack))
    stack_count[formatted_stack] += 1


def sample_memory(timestamp, pid, stream, block_number):
    memory = process_memory_mb(pid)
    stream.write("{timestamp:.6f} {block_number} {memory:.4f}\n".format(
        timestamp=timestamp,
        block_number=block_number,
        memory=memory,
    ))


def sample_objects(timestamp, stream):
    # we don't want to keep the count_per_type object in memory, so instead of
    # creating a global list with all the samples we are append the file with
    # the new data, this has the advantage of "streaming" data and not using
    # too much memory
    count_per_type = objgraph.typestats()

    # add the timestamp for plotting
    data = [timestamp, count_per_type]

    data_pickled = pickle.dumps(data)
    stream.write(data_pickled)


def sample(signum, frame):
    global _sample_profiling, _stack_count, _memory_stream, _block_number, _objects_stream, _last_sample

    # the last pending signal after sample_stop
    if not _sample_profiling:
        return

    timestamp = time.time()

    sample_stack(_stack_count, frame)
    sample_memory(timestamp, os.getpid(), _memory_stream, _block_number)

    # a minute is to coarse for a single session
    if timestamp - _last_sample > 10 * SECOND:
        sample_objects(timestamp, _objects_stream)
        _objects_stream.flush()

    # waiting for the cache to fill takes too long, just flush the data
    _memory_stream.flush()

    _last_sample = timestamp


def sample_toogle():
    global _sample_profiling

    if not _sample_profiling:
        sample_start()
    else:
        sample_stop()


def sample_start():
    global _sample_profiling, _stack_count, _memory_stream, _stack_stream, _objects_stream

    if _sample_profiling:
        return

    _stack_count = collections.defaultdict(int)

    now = datetime.now()
    stack_file = '{:%Y%m%d_%H%M}_stack.data'.format(now)
    memory_file = '{:%Y%m%d_%H%M}_memory.data'.format(now)
    objects_file = '{:%Y%m%d_%H%M}_objects.pickle'.format(now)

    _stack_stream = open(stack_file, 'w')
    _memory_stream = open(memory_file, 'w')
    _objects_stream = open(objects_file, 'w')

    # set this only after startup
    timer = Timer(sample)
    _sample_profiling = timer
    log.info('Starting sample profiling')


def sample_stop():
    global _sample_profiling, _stack_count, _memory_stream, _stack_stream

    if not _sample_profiling:
        return

    # set this before tear down
    del _sample_profiling
    _sample_profiling = False

    stack_data = flamegraph_format(_stack_count)
    _stack_stream.write(stack_data)

    _memory_stream.close()
    _stack_stream.close()
    _memory_stream = None
    _stack_stream = None
    _stack_count = None

    log.info('Stopping sample profiling')


try:
    # < 2.7.8 isnt supported (but seems easy to add)
    # ==2.7.8 needs patching
    # > 3.4   has tracemalloc on the stdlib
    import tracemalloc


    def _serialize_statistics(statistics):
        traceback = [
            frame._frame
            for frame in statistics.traceback
        ]
        return (statistics.count, statistics.size, traceback)


    def sample_trace(signum, frame):
        global _trace_profiling, _trace_stream

        # the last pending signal after trace_stop
        if not _trace_profiling:
            return

        gc.collect()

        snapshot = tracemalloc.take_snapshot()
        timestamp = time.time()
        sample = (timestamp, snapshot)

        # statistics_groupby = snapshot.statistics('lineno')
        # sample = list(map(_serialize_statistics, statistics_groupby))
        # data_pickled = pickle.dumps((timestamp, sample))

        # We _need_ to use the HIGHEST_PROTOCOL, otherwise the serialization
        # will use GBs of memory
        pickle.dump(sample, _trace_stream, protocol=pickle.HIGHEST_PROTOCOL)
        _trace_stream.flush()


    def trace_toggle():
        global _trace_profiling

        # each tracing method takes care of it's own clock
        if not _trace_profiling:
            # stop sampling memory, tracemalloc uses memory itself
            sample_stop()
            trace_start()
        else:
            trace_stop()
            sample_start()


    def trace_start():
        global _trace_profiling, _trace_stream

        if _trace_profiling:
            return

        now = datetime.now()
        trace_file = '{:%Y%m%d_%H%M}_trace.pickle'.format(now)
        _trace_stream = open(trace_file, 'w')
        tracemalloc.start(15)

        # Take snapshots at slower pace because the size of the samples is not
        # negligible, the de/serialization is slow and uses lots of memory.
        _trace_profiling = Timer(sample_trace, interval=MINUTE * 5)

        log.info('Starting trace profiling')


    def trace_stop(restart_sample=True):
        global _trace_profiling, _trace_stream

        if not _trace_profiling:
            return

        del _trace_profiling
        _trace_profiling = False

        tracemalloc.stop()

        _trace_stream.close()
        _trace_stream = None
        log.info('Stoping trace profiling')


except ImportError:
    tracemalloc = None
    def noop(): pass
    trace_start = noop
    trace_stop = noop

    def trace_toogle():
        log.info('Trace profiling is unavailable, install the malloctrace extension')


def cpu_profiling():
    global _cpu_profiling

    if _cpu_profiling is None:
        # create a new file every time instead of overwritting the latest profiling
        # trace_file = '{:%Y%m%d_%H%M}_profile_trace'.format(datetime.now())
        summary_file = '{:%Y%m%d_%H%M}_profile_summary'.format(datetime.now())
        stats_file = '{:%Y%m%d_%H%M}_profile_stats'.format(datetime.now())

        # deterministic profiling can use tons of memory and create unrelated
        # objects, so disable the sampling profiling for now
        sample_stop()

        GreenletProfiler.set_clock_type('cpu')
        GreenletProfiler.start()

        log.info('starting gevent profiling: {}'.format(summary_file))
        _cpu_profiling = True
    else:
        GreenletProfiler.stop()

        greenlet_file = '{:%Y%m%d_%H%M}_profile_greenlet.callgrind'.format(datetime.now())
        stats = GreenletProfiler.get_func_stats()
        stats.print_all()
        stats.save(greenlet_file, type='callgrind')

        # restarts smapling profiling
        sample_start()

        _cpu_profiling = None
        log.info('stopping gevent profiling')


def on_block(block):
    global _block_number, _memory100_stream
    _block_number = block.number

    if not _memory100_stream:
        memory100_file = '{:%Y%m%d_%H%M}_memory_at_1000.data'.format(START)
        _memory100_stream = open(memory100_file, 'w')

    if block.number % 100 == 0:
        memory = process_memory_mb(os.getpid())
        log.info('memory usage at block {}: {}'.format(block.number, memory))
        _memory100_stream.write('{} {} {}\n'.format(time.time(), block.number, memory))
        _memory100_stream.flush()


def plot_date_axis(axes):
    from matplotlib import dates

    date_fmt = dates.DateFormatter('%d/%b')
    hour_fmt = dates.DateFormatter('%H:%M')

    axes.xaxis.set_major_locator(dates.DayLocator(interval=1))
    axes.xaxis.set_major_formatter(date_fmt)
    # less than 5 days and interval of 3 is okay
    axes.xaxis.set_minor_locator(dates.HourLocator(interval=4))  # TODO: set the interval dynamically
    axes.xaxis.set_minor_formatter(hour_fmt)
    axes.xaxis.set_tick_params(which='major', pad=15)


def plot_configure(figure):
    figure.set_figwidth(18)
    figure.set_figheight(4.5)


def memory_objcount(output, data_list, topn=10, figwidth=18):
    import matplotlib.pyplot as plt
    import numpy as np

    TIMESTAMP = 0
    OBJECT_COUNT = 1
    ONESECOND_TIMEDELTA = timedelta(days=0, seconds=1)

    # make sure we are sorted by timestamp
    data_list = sorted(data_list, key=lambda el: el[0][TIMESTAMP])

    # some classes might appear in one sample but not in another, we need to
    # make sure that this class will have all points otherwise the matplotlib
    # call will fail
    alltime_count = sum(len(data) for data in data_list)
    # these extra points are to create the vallyes
    alltime_count += len(data_list) * 2
    alltime_factory = lambda: [0.0 for __ in range(alltime_count)]

    position = 0
    alltime_data = {}
    alltime_timestamps = []
    for data in data_list:
        sample_highcount = {}

        # some classes might not appear on all samples, nevertheless the list
        # must have the same length
        sample_count = len(data)
        sample_factory = lambda: [0.0 for __ in range(sample_count)]
        objcount = collections.defaultdict(sample_factory)

        # group the samples by class
        for index, (__, count_per_type) in enumerate(data):
            for klass, count in count_per_type.items():
                objcount[klass][index] = count

                highcount = max(count, sample_highcount.get(klass, 0))
                sample_highcount[klass] = highcount

        # get topn of the _last_ sample and show only these
        # last_epoch = [
        #     (objcount[klass][-1], klass)
        #     for klass in objcount
        # ]
        # topn_list = sorted(last_epoch, reverse=True)[:topn]

        # get the topn classes with the highest object count, the idea to show
        # spikes
        topn_classes = sorted(
            ((count, klass) for klass, count in sample_highcount.items()),
            reverse=True
        )[:topn]

        # this creates the valley in the graph, we assume that there is a
        # startup interval between sampling files and that the end of the last
        # sample should not continue directly by the next sample, so we need to
        # force the value to zero
        for __, klass in topn_classes:
            if klass in alltime_data:
                points = alltime_data[klass]
            else:
                points = alltime_factory()

            points[position+1:position+len(data)+1] = objcount[klass]
            alltime_data[klass] = points

        sample_timestamps = [sample[TIMESTAMP] for sample in data]
        sample_timestamps.append(sample_timestamps[-1] + ONESECOND_TIMEDELTA)
        sample_timestamps.insert(0, sample_timestamps[0] - ONESECOND_TIMEDELTA)
        alltime_timestamps.extend(sample_timestamps)

        position += len(data) + 2

    fig, axes = plt.subplots()

    # we don't need a point before the first sample and after the last
    labels = alltime_data.keys()
    values = [alltime_data[label] for label in labels]

    # plot only once to share the labels and colors
    axes.stackplot(alltime_timestamps, *values, labels=labels)
    axes.legend(loc='upper left', fontsize='small')

    plot_date_axis(axes)
    plot_configure(fig)
    plt.savefig(output)


def memory_timeline(output, data_list):
    '''Plots all the data in data_list into a single axes, pinning the y-axis
    minimum at zero.

    This plot was created to compare multiple executions of the application,
    removing skew in both axis.
    '''
    import matplotlib.pyplot as plt

    data_list = sorted(data_list, key=lambda list_: list_[0][TIMESTAMP])

    fig, memory_axes = plt.subplots()
    block_axes = memory_axes.twinx()

    last_ts = None
    blocks_max, memory_max = 0, 0

    for data in data_list:
        timestamp = [line[TIMESTAMP] for line in data]
        blocks = [line[BLOCKS] for line in data]
        memory = [line[MEMORY] for line in data]

        memory_max = max(max(memory), memory_max)
        blocks_max = max(max(blocks), blocks_max)

        memory_axes.plot(timestamp, memory, color='b')
        block_axes.plot(timestamp, blocks, color='r')

        if last_ts is not None:
            plt.axvspan(last_ts, timestamp[0], color='y', alpha=0.1, lw=0)

        last_ts = timestamp[-1]

    memory_max *= 1.1
    blocks_max *= 1.1

    memory_axes.set_ylim(0, memory_max)
    block_axes.set_ylim(0, blocks_max)

    plot_date_axis(memory_axes)
    memory_axes.set_ylabel('Memory (MB)', color='b')
    block_axes.set_ylabel('Blocks', color='r')
    plot_configure(fig)
    plt.savefig(output)


def memory_subplot(output, data_list):
    '''Plots all data in separated axes, a simple way to look at distinct
    executions, keep in mind that the time-axis will be skewed, since each plot
    has a differente running time but the same plotting area.'''
    import matplotlib.pyplot as plt
    from matplotlib import dates

    number_plots = len(data_list)
    fig, all_memory_axes = plt.subplots(1, number_plots, sharey='row')

    if number_plots == 1:
        all_memory_axes = [all_memory_axes]

    blocks_max, memory_max = 0, 0
    for line in chain(*data_list):
        memory_max = max(memory_max, line[MEMORY])

        # we might not have the block number
        if line[BLOCKS]:
            blocks_max = max(blocks_max, line[BLOCKS])

    # give room for the lines and axes
    blocks_max *= 1.1  # if there is no data on the block number this will be zero
    memory_max *= 1.1

    hour_fmt = dates.DateFormatter('%H:%M')
    for count, (data, memory_axes) in enumerate(zip(data_list, all_memory_axes)):
        timestamp = [line[TIMESTAMP] for line in data]
        blocks = [line[BLOCKS] for line in data]
        memory = [line[MEMORY] for line in data]

        dt_start_time = timestamp[0]
        hours = timestamp[-1] - dt_start_time
        label = '{start_date:%Y-%m-%d}\n{runtime}'.format(start_date=dt_start_time, runtime=hours)

        memory_axes.plot(timestamp, memory, color='b')
        memory_axes.set_ylim(0, memory_max)
        memory_axes.xaxis.set_major_formatter(hour_fmt)
        memory_axes.set_xlabel(label)

        if len(data_list) == 1 or count == 0:
            memory_axes.set_ylabel('Memory (MB)')
        else:
            memory_axes.get_yaxis().set_visible(False)

        if blocks_max != 0:
            block_axes = memory_axes.twinx()
            block_axes.plot(timestamp, blocks, color='r')
            block_axes.set_ylim(0, blocks_max)

            if len(data_list) == 1 or count == len(data_list) - 1:
                block_axes.set_ylabel('Blocks')
            else:
                block_axes.get_yaxis().set_visible(False)

    fig.autofmt_xdate()
    plot_configure(fig)
    plt.savefig(output)


def latency_scatter(output, data_list, interval):
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots()

    data_list = sorted(data_list, key=lambda list_: list_[0])

    timestamp, latency = [], []
    for timestamps in data_list:
        last = timestamps.pop(0)
        for current in timestamps:
            timedelta = current - last
            # seconds = timedelta.total_seconds() - interval
            seconds = timedelta.total_seconds()
            timestamp.append(current)
            latency.append(seconds)
            last = current

    if latency:
        axes.set_ylabel('Latency (sec)')
        axes.scatter(timestamp, latency, s=10, alpha=0.1, marker=',', edgecolors='none')
        axes.set_xlim(min(timestamp), max(timestamp))
        axes.set_ylim(0, max(latency) * 1.1)

    last_ts = None
    for timestamps in data_list:
        if not timestamps:
            continue

        if last_ts is not None:
            plt.axvspan(last_ts, timestamps[0], color='y', alpha=0.1, lw=0)

        last_ts = timestamps[-1]

    plot_date_axis(axes)
    plot_configure(fig)
    plt.savefig(output)


def objcount_data(filepath):
    import pickle

    # the file dosn't contain just one pickled object, but a sequence of
    # pickled dictionaries (look the sample_objects for details)
    #
    # note: this only works with objects that keep track of the reading
    # position

    data = []
    try:
        with open(filepath) as handler:
            while True:  # while we dont hit EOFError
                timestamp_string, object_count = pickle.load(handler)
                timestamp = ts_to_dt(timestamp_string)
                line = (timestamp, object_count)

                if line:
                    data.append(line)
    except EOFError:
        # we loaded all the objects from the file
        pass

    return data


def memory_data(filepath):
    def convert_line(line):
        '''returns a tuple (timestamp, block_number, memory_usage)'''
        return (
            ts_to_dt(line[TIMESTAMP]),
            int(line[BLOCKS]) if line[BLOCKS] else None,
            float(line[MEMORY]),
        )

    with open(filepath) as handler:
        # the format of the file is three columns:
        # timestamp block.number memory_mb
        data = [
            line.split()
            for line in handler
            if line
        ]

    # TODO: fix the script to lookup the current block.number
    # get the first block_number (the block_number variable might be equal
    # to None if the profiling was started before we got a new block)
    first = None
    for line in data:
        if line[BLOCKS] != 'None':
            first = str(int(line[BLOCKS]) - 1)
            break

    for item in data:
        if item[BLOCKS] == 'None':
            item[BLOCKS] = first
        else:
            break

    return map(convert_line, data)


def latency_data(filepath):
    with open(filepath) as handler:
        return [
            ts_to_dt(line.split()[0])
            for line in handler
            if line
        ]


if __name__ != '__main__':
    # always run the sampling profiling, because of this we want to flush the
    # streams
    sample_start()

    atexit.register(sample_stop)
    atexit.register(trace_stop)

    gevent.signal(signal.SIGUSR1, cpu_profiling)
    gevent.signal(signal.SIGUSR2, trace_toggle)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()

    # first action
    action_parser = parser.add_subparsers(dest='action')
    memory_parser = action_parser.add_parser('memory')
    latency_parser = action_parser.add_parser('latency')

    # memory subaction
    memory_plot_parser = memory_parser.add_subparsers(dest='plot')
    memory_subplot_parser = memory_plot_parser.add_parser('subplot')
    memory_timeline_parser = memory_plot_parser.add_parser('timeline')
    memory_objcount_parser = memory_plot_parser.add_parser('objcount')

    memory_subplot_parser.add_argument('data', nargs='+')
    memory_subplot_parser.add_argument('--output', default='memory.png', type=str)

    memory_timeline_parser.add_argument('data', nargs='+')
    memory_timeline_parser.add_argument('--output', default='memory_timeline.png', type=str)

    memory_objcount_parser.add_argument('data', nargs='+')
    memory_objcount_parser.add_argument('--output', default='memory_objcount.png', type=str)
    memory_objcount_parser.add_argument('--topn', default=10, type=int)

    # latency subaction
    latency_plot_parser = latency_parser.add_subparsers(dest='plot')
    latency_scatter_parser = latency_plot_parser.add_parser('scatter')

    latency_scatter_parser.add_argument('data', nargs='+')
    latency_scatter_parser.add_argument('--interval', default=INTERVAL_SECONDS, type=float)
    latency_scatter_parser.add_argument('--output', default='latency_scatter.png', type=str)

    arguments = parser.parse_args()

    # consistent styling
    import matplotlib.pyplot as plt
    plt.style.use('ggplot')

    if arguments.action == 'memory' and arguments.plot == 'subplot':
        data_list = [
            memory_data(path)
            for path in arguments.data
        ]
        data_list = filter(len, data_list)
        memory_subplot(arguments.output, data_list)

    elif arguments.action == 'memory' and arguments.plot == 'timeline':
        data_list = [
            memory_data(path)
            for path in arguments.data
        ]
        data_list = filter(len, data_list)
        memory_timeline(arguments.output, data_list)

    elif arguments.action == 'memory' and arguments.plot == 'objcount':
        data_list = [
            objcount_data(path)
            for path in arguments.data
        ]
        data_list = filter(len, data_list)
        memory_objcount(arguments.output, data_list, topn=arguments.topn)

    elif arguments.action == 'latency' and arguments.plot == 'scatter':
        data_list = [
            latency_data(path)
            for path in arguments.data
        ]
        data_list = filter(len, data_list)
        latency_scatter(arguments.output, data_list, arguments.interval)
