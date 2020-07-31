#!/usr/bin/env python

import queue
import logging
import sys
import threading

import six

import koji
import koji.tasks
from koji.plugin import export_cli
from koji_cli.lib import OptionParser, _, watch_tasks, activate_session


REPOCACHE = {}




@export_cli
def handle_replicate_tasks(options, session, args):
    """[admin] Replicate tasks"""
    logger = logging.getLogger('koji.replicateTasks')

    (parser, opts, task_ids) = parse_options(options, args)
    if options.debug:
        logger.setLevel('DEBUG')
    task_ids = check_options(parser, opts, task_ids)
    activate_session(session, options)
    channel_override = getattr(opts, 'channel_override', None)
    if channel_override:
        opts.channels_override = session.getChannel(channel_override,
                                                    strict=True)['id']
    if not task_ids:
        tasks = get_tasks(session, parser, opts)
    else:
        tasks = task_ids
    b_queue = queue.Queue()
    for task in tasks:
        b_queue.put(task)
    threads = []
    for i in range(4):
        subsession = session.subsession()
        thread = threading.Thread(name='replicator %i' % i,
                                  target=replicate_handler,
                                  args=(subsession, b_queue, opts))
        thread.setDaemon(True)
        thread.start()
        threads.append(thread)
    for t in threads:
        t.join()


def get_tasks(session, parser, opts):
    tasks = []
    channels = opts.channels
    hosts = opts.hosts
    methods = opts.methods
    states = getattr(opts, 'states', None)
    limit = getattr(opts, 'limit', None)
    offset = getattr(opts, 'offset', None)
    channel_ids = []
    if channels:
        for channel in channels:
            channel_ids.append(session.getChannel(channel, strict=True)['id'])
    host_ids = []
    if hosts:
        for host in hosts:
            host_ids.append(session.getHost(host, strict=True)['id'])
    state_nums = []
    for state in states:
        if isinstance(state, six.integer_types):
            if 0 <= state <= 5:
                state_nums.append(state)
            else:
                raise koji.GenericError("integer state should >=0 and <=5")
        elif isinstance(state, six.string_types):
            state_nums.append(koji.TASK_STATES[state])
        else:
            raise koji.GenericError("unacceptable state type")
    options = {}
    if state_nums:
        options['state'] = state_nums
    options['parent'] = None
    options['decode'] = True

    queryOpts = {}
    if limit:
        queryOpts['limit'] = limit
    if offset:
        queryOpts['offset'] = offset
    queryOpts['order'] = '-id'
    session.multicall = True
    if channel_ids:
        for channel_id in channel_ids:
            if methods:
                for method in methods:
                    session.listTasks(dict(options, method=method,
                                           channel_id=channel_id),
                                      queryOpts)
            else:
                session.listTasks(dict(options, channel_id=channel_id),
                                  queryOpts)

    elif host_ids:
        for host_id in host_ids:
            if methods:
                for method in methods:
                    session.listTasks(dict(options, method=method,
                                           host_id=host_id),
                                      queryOpts)
            else:
                session.listTasks(dict(options, host_id=host_id),
                                  queryOpts)
    else:
        if methods:
            for method in methods:
                session.listTasks(dict(options, method=method), queryOpts)
        else:
            session.listTasks(options, queryOpts)
    tasks = sum(sum(session.multiCall(), []), tasks)
    if not tasks:
        raise koji.GenericError("no tasks to replicate.")
    return tasks


def parse_options(options, args):
    usage = _("usage: %prog replicate-tasks [options] [<task_id>...]")
    usage += _(
        "\nto replicate scratch tasks from existing tasks with specified IDs"
        " or by query"
        "\n(Specify the --help global option for a list of"
        " other help options)")
    parser = OptionParser(usage=usage)
    parser.disable_interspersed_args()
    parser.add_option("-C", "--channel", dest="channels", action="append",
                      default=[],
                      help=_("specify channels where tasks are from"))
    parser.add_option("-H", "--host", dest="hosts", action="append",
                      default=[],
                      help=_("specify hosts where tasks are replicated from"))
    parser.add_option("-m", "--method", dest="methods", action="append",
                      default=[],
                      help=_("specify methods that original tasks are"))
    parser.add_option("-S", "--state", dest="states", action="append",
                      default=['CLOSED'],
                      help=_("specify states of tasks which are replicated"))
    parser.add_option("-w", "--weight", type='int', help=_("set task weight"))
    parser.add_option("--channel-override",
                      help=_("use a non-standard channel to replicate tasks"))
    parser.add_option("--arch-override", dest="arches", action="append",
                      default=[],
                      help=_("to override arches to replicate tasks"))
    parser.add_option("--include-scratch", action="store_true",
                      help=_("also replicate scratch tasks"))
    # parser.add_option("--limit-by", default='channel',
    #                   help=_("specify field used by --limit"))
    parser.add_option("--limit", type='int', default=3,
                      help=_("max task count per channel/host"))
    parser.add_option("--offset", type="int", default=0,
                      help=_("offset of limit"))
    parser.add_option("--quiet", action="store_true", default=options.quiet,
                      help=_("Do not print the task information"))

    return (parser,) + parser.parse_args(args)


def check_options(parser, options, args):
    ints = []
    try:
        for arg in args:
            ints.append(int(arg))
    except ValueError:
        parser.error(_("Only task ids are accepted as arguments"))
    if options.channels and options.hosts:
        parser.error(_("Options: --channel and --host are conflicted"))
    return ints


def replicate_build_task(session, task, options):
    if isinstance(task, six.integer_types):
        task = session.getTaskInfo(task, request=True)
    task_id = task['id']
    print("%i: Looking at task" % task_id)
    if task['parent'] is not None:
        raise koji.GenericError("%(id)i: not a parent task" % task)
    if task['method'] == 'build':
        arglist = replicate_build_request(session, task, options)
    elif task['method'] in ['image', 'livemedia', 'livecd', 'appliance']:
        arglist = replicate_image_request(session, task, options)
    else:
        raise koji.GenericError("%(id)i: can not replicate %(method)s task" % task)
    channel = task['channel_id']
    if options.channel_override:
        channel = options.channel_override
    new_task_id = session.makeTask(task['method'], arglist, channel=channel)
    print("Original task %i replicated as task %i" % (task_id, new_task_id))
    rv = watch_tasks(session, [new_task_id], quiet=options.quiet)
    return True


def replicate_build_request(session, task, options):
    task_id = task['id']
    include_scratch = getattr(options, 'include_scratch', False)
    arches = options.arches
    orig_repo = None
    for subtask in session.getTaskChildren(task_id, request=True):
        if subtask['method'] != 'buildArch':
            continue
        sub_params = koji.tasks.parse_task_params(subtask['method'],
                                                  subtask['request'])
        sub_opts = sub_params.get('opts', {})
        if not orig_repo:
            orig_repo = sub_opts.get('repo_id')

    # duplicate build task
    params = koji.tasks.parse_task_params(task['method'], task['request'])
    opts = params.get('opts', {})
    noopts = bool(opts)
    if not include_scratch and opts.get('scratch'):
        raise koji.GenericError("#%i: Skipping scratch build" % task_id)

    if not orig_repo:
        raise koji.GenericError(
            "#%i: Could not determine original repo" % task_id)
    repo_info = replicate_repo(session, orig_repo)

    opts['repo_id'] = repo_info['id']
    opts['scratch'] = True
    if arches:
        opts['arch_override'] = ' '.join(arches)
        print("%i: override arches: %s" % (task_id, arches))
    request = task['request']
    if noopts:
        request.append(opts)
    else:
        request[-1] = opts
    return request


def replicate_image_request(session, task, options):
    task_id = task['id']
    include_scratch = getattr(options, 'include_scratch', False)
    arches = options.arches

    params = koji.tasks.parse_task_params(task['method'], task['request'])
    opts = params.get('opts', {})
    noopts = bool(opts)
    if not include_scratch and opts.get('scratch'):
        raise koji.GenericError("#%i: Skipping scratch build" % task_id)
    opts['scratch'] = True
    request = task['request']
    if arches:
        request[2] = arches
        print("%i: override arches: %s" % (task_id, arches))
    if noopts:
        request.append(opts)
    else:
        request[-1] = opts
    return request


def replicate_repo(session, repo_id):
    global REPOCACHE
    while repo_id in REPOCACHE:
        repo_id = REPOCACHE[repo_id]
    repo_info = session.repoInfo(repo_id, strict=True)

    if koji.REPO_STATES[repo_info['state']] in ['READY', 'EXPIRED']:
        # we'll just reuse this repo
        return repo_info
    # otherwise
    print("Duplicating repo#%i" % repo_id)
    rtaskid = session.newRepo(repo_info['tag_name'],
                              event=repo_info['create_event'])
    # XXX ^ switch to tag_id in future
    watch_tasks(session, [rtaskid])
    new_repo_id, event_id = session.getTaskResult(rtaskid)
    repo_info = session.repoInfo(new_repo_id)
    REPOCACHE.setdefault(repo_id, new_repo_id)
    return repo_info


def replicate_handler(session, b_queue, options):
    while True:
        try:
            taskinfo = b_queue.get(False)
        except queue.Empty:
            break
        if isinstance(taskinfo, six.integer_types):
            task_id = taskinfo
        else:
            task_id = taskinfo['id']
        print("Replicating build from task #%s" % task_id)
        replicate_build_task(session, taskinfo, options)
