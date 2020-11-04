import queue
import logging
import threading
from enum import Enum
from functools import reduce

import six

import koji
import koji.tasks
from koji.plugin import export_cli
from koji_cli.lib import OptionParser, _, watch_tasks, activate_session


REPOCACHE = {}

logger = logging.getLogger('koji.replicateTasks')


class Strategy(Enum):
    # reusing the *versioned* build tag of the original task
    reuse = 0
    # cloning the *versioned* group/pkglist/build to a new tag, then the build tag inherits from it
    # and a overriiden-tag specified by --override-tag, where extra_config and others can be
    # overridden.
    clone = 1


@export_cli
def handle_replicate_tasks(options, session, args):
    """[admin] Replicate tasks"""
    (parser, opts, task_ids) = parse_options(options, args)
    if options.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
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
    cvs = []
    with session.multicall() as m:
        if channel_ids:
            for channel_id in channel_ids:
                if methods:
                    for method in methods:
                        cvs.append((m.listTasks(dict(options, method=method,
                                                     channel_id=channel_id),
                                                queryOpts)))
                else:
                    cvs.append(m.listTasks(dict(options, channel_id=channel_id), queryOpts))
        elif host_ids:
            for host_id in host_ids:
                if methods:
                    for method in methods:
                        cvs.append(m.listTasks(dict(options, method=method, host_id=host_id),
                                               queryOpts))
                else:
                    cvs.append(m.listTasks(dict(options, host_id=host_id), queryOpts))
        else:
            if methods:
                for method in methods:
                    cvs.append(m.listTasks(dict(options, method=method), queryOpts))
            else:
                cvs.append(m.listTasks(options, queryOpts))
    tasks = sum([cv.result for cv in cvs], [])
    print(tasks)
    if not tasks:
        raise koji.GenericError("no tasks to replicate.")
    else:
        logger.debug("to replicate tasks:\n%s", "\n".join([str(t['id']) for t in tasks]))
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
    parser.add_option("-s", "--strategy", default=Strategy.reuse.name,
                      help=_("specify the strategy to construct the buildroot for"
                             " replicating the task, Options: %s,"
                             " [Default: %%default]." % ", ".join(Strategy.__members__.keys())))
    parser.add_option("-T", "--override-tag",
                      help=_("specify the tag in the inheritance to override the content / config"
                             " of the origin build tag when strategy is clone"))
    parser.add_option("-C", "--channel", dest="channels", action="append",
                      default=[],
                      help=_("specify channels where tasks are from"))
    parser.add_option("-H", "--host", dest="hosts", action="append",
                      default=[],
                      help=_("specify hosts where tasks are replicated from"))
    parser.add_option("-m", "--method", dest="methods", action="append",
                      default=[],
                      help=_("specify methods that original tasks are. Only supports 'build' now"))
    parser.add_option("-S", "--state", dest="states", action="append",
                      default=['CLOSED'],
                      help=_("specify states of tasks which are replicated, [Default: %default]"))
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
                      help=_("limit per method and/or per channel/host, [Default: %default]"))
    parser.add_option("--offset", type="int", default=0,
                      help=_("offset of limit, [Default: %default]"))
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
    if options.strategy not in Strategy.__members__.keys():
        parser.error(_("--strategy must be one of %s" % ", ".join(Strategy.__members__.keys())))
    if options.override_tag and options.strategy != Strategy.clone.name:
        parser.error(_("--override-tag is only available when --strategy=%s"
                       % Strategy.clone.name))
    return ints


def replicate_build_task(session, task, options):
    if isinstance(task, six.integer_types):
        task = session.getTaskInfo(task, request=True)
    task_id = task['id']
    logger.info("%i: Looking at task", task_id)
    if task['parent'] is not None:
        raise koji.GenericError("%(id)i: not a parent task" % task)
    if task['method'] == 'build':
        params = replicate_build_request(session, task, options)
    # TODO: ONLY build task is supported right now
    # elif task['method'] in ['image', 'livemedia', 'livecd', 'appliance']:
    #     params = replicate_image_request(session, task, options)
    else:
        raise koji.GenericError("%(id)i: can not replicate %(method)s task" % task)
    channel = task['channel_id']
    if options.channel_override:
        channel = options.channel_override
    new_task_id = session.makeTask(task['method'], koji.encode_args(**params), channel=channel)
    logger.info("Original task %i replicated as task %i", task_id, new_task_id)
    rv = watch_tasks(session, [new_task_id], quiet=options.quiet)
    # always True
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
        logger.debug('[task#%s] sub-params: %s', task_id, sub_params)
        sub_opts = sub_params.get('opts', {})
        if not orig_repo:
            orig_repo = sub_opts.get('repo_id', None)

    # duplicate build task
    params = koji.tasks.parse_task_params(task['method'], task['request'])
    request = params.copy()
    opts = params.get('opts', {})
    if not include_scratch and opts.get('scratch'):
        raise koji.GenericError("#%i: Skipping scratch build" % task_id)

    if not orig_repo:
        raise koji.GenericError(
            "#%i: Could not determine original repo" % task_id)
    if options.strategy == Strategy.reuse.name:
        repo_info = replicate_repo(session, orig_repo)
    elif options.strategy == Strategy.clone.name:
        repo_info = clone_tag(session, orig_repo, task_id, options.override_tag)
        request['target'] = None
    else:
        raise koji.GenericError("strategy: %s is incorrect (NEVER HAPPENS)" % options.strategy)
    opts['repo_id'] = repo_info['id']
    opts['scratch'] = True
    if arches:
        opts['arch_override'] = ' '.join(arches)
        logger.info("%i: override arches: %s", task_id, arches)
    request['opts'] = opts
    return request


# TODO: finish this
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
        logger.info("%i: override arches: %s", task_id, arches)
    if noopts:
        request.append(opts)
    else:
        request[-1] = opts
    return request


def replicate_repo(session, repo_id, tag_id=None):
    global REPOCACHE
    logger.info("Replicating repo: #%s, tag: #%s", repo_id, tag_id)
    orig_repo_id = repo_id
    # FOUND
    if repo_id in REPOCACHE:
        repo_id = REPOCACHE[repo_id]
        repo_info = session.repoInfo(repo_id, strict=True)
        if koji.REPO_STATES[repo_info['state']] in ['READY', 'EXPIRED']:
            # we'll just reuse this repo
            return repo_info
        else:
            # cached repo has been deleted, regen it!
            logger.info('[DELETED] Duplicate repo: #%s, based on: repo: %s ' % (orig_repo_id,
                                                                                repo_info))
            return new_repo(session, None, repo_info, orig_repo_id)
    # NOT FOUND
    return new_repo(session, tag_id, repo_id, orig_repo_id)


def new_repo(session, tag, src_repo, orig_repo_id):
    # cloning
    # tag_id is the ID of cloned tag
    if tag:
        act_repo = session.getRepo(tag, event=None)
        if act_repo:
            REPOCACHE.setdefault(orig_repo_id, act_repo['id'])
            return session.repoInfo(act_repo['id'])
        event = None
    # reusing/refreshing
    else:
        if isinstance(src_repo, six.integer_types):
            src_repo = session.repoInfo(src_repo)
        if src_repo and isinstance(src_repo, dict):
            event = src_repo['create_event']
            tag = src_repo['tag_id']
    rtaskid = session.newRepo(tag, event=event)
    watch_tasks(session, [rtaskid])
    new_repo_id, event_id = session.getTaskResult(rtaskid)
    repo_info = session.repoInfo(new_repo_id)
    REPOCACHE.setdefault(orig_repo_id, new_repo_id)
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
        logger.info("Replicating build from task #%s", task_id)
        try:
            replicate_build_task(session, taskinfo, options)
        except Exception:
            logger.error("An Error occurs when replicating build from task #%s", task_id,
                         exc_info=True)


def clone_tag(session, repo_id, task_id, override_tag_name=None):
    if override_tag_name:
        override_tag = session.getTag(override_tag_name, strict=True)
    # create the tag
    prefix = 'task-replication-%s-' % task_id
    base_tag_name = prefix + 'base'
    build_tag_name = prefix + 'build'
    orig_repo_info = session.repoInfo(repo_id, strict=True)
    event = orig_repo_info['create_event']
    orig_tag = session.getTag(orig_repo_info['tag_id'], strict=True, event=event)
    arches = orig_tag['arches']
    extra = orig_tag['extra']
    extra['cloned_base_tag'] = True
    base_tag = session.getTag(base_tag_name, strict=False)
    if not base_tag:
        logger.info("base tag: %s doesn't exist, creating one", base_tag_name)
        base_tag_id = session.createTag(base_tag_name, arches=arches, extra=extra)
        force = False
    else:
        base_tag_id = base_tag['id']
        force = True
        logger.info("base tag: %s(#%s) exists", base_tag_name, base_tag_id)
    # set the package list
    dup_package_list(session, orig_tag, base_tag_id, event, force=force)
    # tag our builds
    dup_builds(session, orig_tag, base_tag_id, event, force=force)
    # groups data
    dup_groups(session, orig_tag, base_tag_id, event, force=force)

    base_tag = session.getTag(base_tag_id, strict=True)
    build_tag = session.getTag(build_tag_name, strict=False)
    if not build_tag:
        build_tag_id = session.createTag(build_tag_name, arches=arches,
                                         extra={'clonded_build_tag': True})
        build_tag = session.getTag(build_tag_id, strict=True)
    else:
        session.editTag2(build_tag['id'], arches=arches, extra={'clonded_build_tag': True})
    old_inheritance = session.getInheritanceData(build_tag['id'])
    for p in old_inheritance:
        del p['name']
    links = []
    if override_tag:
        links.append({
            'parent_id': override_tag['id'],
            'priority': 5,
            'maxdepth': None,
            'intransitive': False,
            'noconfig': False,
            'pkg_filter': '',
        })
    links.append({
        'parent_id': base_tag['id'],
        'priority': 15,
        'maxdepth': None,
        'intransitive': False,
        'noconfig': False,
        'pkg_filter': '',
    })
    comp = [a for a in links if a in old_inheritance]
    if comp != links:
        session.setInheritanceData(build_tag['id'], links)

    return replicate_repo(session, repo_id, build_tag['id'])


def dup_package_list(session, orig_tag, tag_id, event_id, force=False):
    pkgs = session.listPackages(tagID=orig_tag['id'], inherited=True, event=event_id)
    with session.multicall() as m:
        for pkg in pkgs:
            m.packageListAdd(tag_id, pkg['package_id'], owner=pkg['owner_id'],
                             block=pkg['blocked'], extra_arches=pkg['extra_arches'],
                             force=force, update=False)


def dup_builds(session, orig_tag, tag_id, event_id, force=False):
    builds = session.listTagged(orig_tag['id'], inherit=True, latest=True, event=event_id)
    with session.multicall() as m:
        for binfo in builds:
            m.tagBuildBypass(tag_id, binfo['id'], force=force, notify=False)


def dup_groups(session, orig_tag, tag_id, event_id, force=False):
    groups = session.getTagGroups(orig_tag['id'], event=event_id)
    with session.multicall() as m:
        for grp in groups:
            m.groupListAdd(tag_id, grp['name'], block=bool(grp['blocked']), force=False, **grp)
            # note: groupListAdd ignores the extra fields in **grp
            for pkg in grp['packagelist']:
                m.groupPackageListAdd(tag_id, grp['name'], pkg['package'], block=pkg['blocked'],
                                      force=force, **pkg)
            for req in grp['grouplist']:
                m.groupReqListAdd(tag_id, grp['name'], req['req_id'], block=req['blocked'],
                                  force=force, **req)
