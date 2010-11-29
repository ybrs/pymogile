# -*- coding: utf-8 -*-
"""
implement from description of MogileFS-Client version 1.13
http://search.cpan.org/~dormando/MogileFS-Client/lib/MogileFS/Client.pm
"""
import logging

from pymogile.backend import Backend
from pymogile.exceptions import MogileFSError, MogileFSTrackerError
from pymogile.http import NewHttpFile, ClientHttpFile

logger = logging

def _complain_ifreadonly(readonly):
  if readonly:
    raise ValueError("operation on read-only client")


class Client(object):
  def __init__(self, domain, hosts, readonly=False):
    self.readonly = bool(readonly)
    self.domain   = domain
    self.backend  = Backend(hosts, timeout=3)

  @property
  def last_tracker(self):
    """
    Returns a tuple of (ip, port), representing the last mogilefsd
    'tracker' server which was talked to.
    """
    return self.backend.get_last_tracker()

  def new_file(self, key, cls=None, bytes=0, largefile=False, 
               create_open_arg=None, create_close_arg=None, opts=None):
    """
    Start creating a new filehandle with the given key, 
    and option given class and options.
    
    Returns a filehandle you should then print to, 
    and later close to complete the operation. 
    
    NOTE: check the return value from close! 
    If your close didn't succeed, the file didn't get saved!
    """
    self.run_hook('new_file_start', key, cls, opts)

    create_open_arg = create_open_arg or {}
    create_close_arg = create_close_arg or {}

    # fid should be specified, or pass 0 meaning to auto-generate one
    fid = 0
    params = {'domain'  : self.domain,
              'key'     : key,
              'fid'     : fid,
              'multi_dest': 1}
    if cls is not None:
      params['class'] = cls
    res = self.backend.do_request('create_open', params)
    if not res:
      return None

    # [ (devid,path), (devid,path), ... ]
    dests = []
    # determine old vs. new format to populate destinations
    if 'dev_count' not in res:
      dests.append((res['devid'], res['path']))
    else:
      for x in xrange(1, int(res['dev_count']) + 1):
        devid_key = 'devid_%d' % x
        path_key = 'path_%s' % x
        dests.append((res[devid_key], res[path_key]))

    main_dest = dests[0]
    main_devid, main_path = main_dest

    self.run_hook("new_file_end", key, cls, opts)

    # TODO
    if largefile:
      file_class = ClientHttpFile
    else:
      file_class = NewHttpFile

    return file_class(mg=self,
                      fid=res['fid'],
                      path=main_path,
                      devid=main_devid,
                      backup_dests=dests,
                      cls=cls,
                      key=key,
                      content_length=bytes,
                      create_close_arg=create_close_arg,
                      overwrite=1)

  def read_file(self, *args, **kwds):
    """
    Read the file with the the given key.
    Returns a seekable filehandle you can read() from. 
    Note that you cannot read line by line using <$fh> notation.
    
    Takes the same options as get_paths 
    (which is called internally to get the URIs to read from).
    """
    paths = self.get_paths(*args, **kwds)
    path = paths[0]
    backup_dests = [(None, p) for p in paths[1:]]
    return ClientHttpFile(path=path, backup_dests=backup_dests, readonly=1)

  def get_paths(self, key, noverify=1, zone='alt', pathcount=None):
    self.run_hook('get_paths_start', key)

    if not pathcount:
      pathcount = 2
      
    params = {'domain'   : self.domain,
              'key'      : key,
              'noverify' : noverify and 1 or 0, 
              'zone'     : zone,
              'pathcount': pathcount}
    try:
      res = self.backend.do_request('get_paths', params)
      paths = [res["path%d" % x] for x in xrange(1, int(res["paths"]) + 1)]
    except (MogileFSTrackerError, MogileFSError):
      paths = []

    self.run_hook('get_paths_end', key)
    return paths

  def get_file_data(self, key, timeout=10):
    """
    Returns scalarref of file contents in a scalarref.
    Don't use for large data, as it all comes back to you in one string.
    """
    fp = self.read_file(key, noverify=1)
    try:
      content = fp.read()
      return content
    finally:
      fp.close()

  def rename(self, old_key, new_key):
    """
    Rename file (key) in MogileFS from oldkey to newkey. 
    Returns true on success, failure otherwise
    """
    _complain_ifreadonly(self.readonly)
    self.backend.do_request('rename', {'domain'  : self.domain,
                                       'from_key': old_key,
                                       'to_key'  : new_key})
    return True

  def list_keys(self, prefix=None, after=None, limit=None):
    """
    Used to get a list of keys matching a certain prefix.
    
    $prefix specifies what you want to get a list of. 
    
    $after is the item specified as a return value from this function last time 
          you called it. 
    
    $limit is optional and defaults to 1000 keys returned.
    
    In list context, returns ($after, $keys). 
    In scalar context, returns arrayref of keys. 
    The value $after is to be used as $after when you call this function again.
    
    When there are no more keys in the list, 
    you will get back undef or an empty list
    """
    params = {'domain': self.domain}
    if prefix:
      params['prefix'] = prefix
    if after:
      params['after'] = after
    if limit:
      params['limit'] = limit

    res = self.backend.do_request('list_keys', params)
    reslist = []
    for x in xrange(1, int(res['key_count']) + 1):
      reslist.append(res['key_%d' % x])
    return reslist

  def foreach_key(self, *args, **kwds):
    raise NotImplementedError()
  
  def update_class(self, *args, **kwds):
    raise NotImplementedError()

  def sleep(self, duration):
    """
    just makes some sleeping happen.  first and only argument is number of
    seconds to instruct backend thread to sleep for.
    """
    self.backend.do_request("sleep", {'duration': duration})
    return True

  def set_pref_ip(self, *ips):
    """
    Weird option for old, weird network architecture.  Sets a mapping
    table of preferred alternate IPs, if reachable.  For instance, if
    trying to connect to 10.0.0.2 in the above example, the module would
    instead try to connect to 10.2.0.2 quickly first, then then fall back
    to 10.0.0.2 if 10.2.0.2 wasn't reachable.
    expects as argument a tuple of ("standard-ip", "preferred-ip")
    """
    self.backend.set_pref_ip(*ips)

  def store_file(self, key, fp, cls=None, **opts):
    """
    Wrapper around new_file, print, and close.

    Given a key, class, and a filehandle or filename, stores the file
    contents in MogileFS.  Returns the number of bytes stored on success,
    undef on failure.
    """
    _complain_ifreadonly(self.readonly)

    self.run_hook('store_file_start', key, cls, opts)

    try:
      output = self.new_file(key, cls, largefile=1, **opts)
      bytes = 0
      while 1:
        buf = fp.read(1024 * 16)
        if not buf:
          break
        bytes += len(buf)
        output.write(buf)

      self.run_hook('store_file_end', key, cls, opts)
    finally:
      # finally
      fp.close()
      output.close()

    return bytes

  def store_content(self, key, content, cls=None, **opts):
    """
    Wrapper around new_file, print, and close.  Given a key, class, and
    file contents (scalar or scalarref), stores the file contents in
    MogileFS. Returns the number of bytes stored on success, undef on
    failure.
    """
    _complain_ifreadonly(self.readonly)

    self.run_hook('store_content_start', key, cls, opts)

    output = self.new_file(key, cls, None, **opts)
    try:
      output.write(content)
    finally:
      output.close()

    self.run_hook('store_content_end', key, cls, opts)

    return len(content)

  def delete(self, key):
    """ Delete a key from MogileFS """
    _complain_ifreadonly(self.readonly)
    self.backend.do_request('delete', {'domain': self.domain, 'key': key})
    return True
