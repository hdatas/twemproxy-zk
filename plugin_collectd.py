#! /usr/bin/env python


import collectd
import socket
import json
from datetime import timedelta


class KVProxyPlugin(object):
  """
    This class collects KV proxy stats info, and passes to Collectd.
  """

  def __init__(self):
    self.server = '127.0.0.1'
    self.port = '22222'
    # default interval is 20 seconds.
    self.interval = 20
    self.plugin_name = 'distkvproxy'

  def config(self, conf):
    """
      Config callback.

      Collectd will call this method to init the plugin.

      :param conf: a Config object containing all info representing the
                   config tree.
      For how to inteprete Config object, see here:
      https://collectd.org/documentation/manpages/collectd-python.5.shtml
    """
    for node in conf.children:
      if node.key == 'Host':
        self.server = node.values[0]
      elif node.key == 'Port':
        self.port = int(node.values[0])
      elif node.key == 'Interval':
        self.interval = float(node.values[0])
      else:
        collectd.warning("KVProxyPlugin: Unkown configuration key %s"
                         % node.key)


  def submit(self, type, type_instance, value, server, port):
    plugin_instance = '%s_%s' % (self.port, server)

    v = collectd.Values()
    v.plugin = self.plugin_name
    v.plugin_instance = plugin_instance
    v.type = type
    v.type_instance = type_instance
    v.values = [value, ]
    v.dispatch()


  def parse_server(self, sname, server):
    """
      Parse proxy stats info about a server, then send to collectd

      :param sname:  backend server name
      :param server: json obj representing a server stats.
    """
    submit('connections',
           'server_connections_%s' % sname,
           str(server['server_connections']),
           self.server, self.port)
    submit('error',
           'server_eof_%s' % sname,
           str(server['server_eof']),
           self.server, self.port)
    submit('error',
           'server_err_%s' % sname,
           str(server['server_err']),
           self.server, self.port)
    submit('count',
           'requests_%s' % sname,
           str(server['requests']),
           self.server, self.port)
    submit('count',
           'request_bytes_%s' % sname,
           str(server['request_bytes']),
           self.server, self.port)
    submit('count',
           'responses_%s' % sname,
           str(server['responses']),
           self.server, self.port)
    submit('count',
           'response_bytes_%s' % sname,
           str(server['response_bytes']),
           self.server, self.port)
    submit('count',
           'in_queue_%s' % sname,
           str(server['in_queue']),
           self.server, self.port)
    submit('count',
           'in_queue_bytes_%s' % sname,
           str(server['in_queue_bytes']),
           self.server, self.port)
    submit('count',
           'out_queue_%s' % sname,
           str(server['out_queue']),
           self.server, self.port)
    submit('count',
           'out_queue_bytes_%s' % sname,
           str(server['out_queue_bytes']),
           self.server, self.port)


  def parse_pool(self, pname, pool):
    """
      Parse proxy stats info about a KV pool, then send to collectd

      :param pname: pool name
      :param pool:  json obj representing pool stats.
    """

    # First, record top summaries for this pool.
    submit('connections', 'client_connections', str(pool['client_connections']),
           self.server, self.port)
    submit('error', 'client_err', str(pool['client_err']),
           self.server, self.port)
    submit('error', 'client_eof', str(pool['client_eof']),
           self.server, self.port)
    submit('error', 'server_ejects', str(pool['server_ejects']),
           self.server, self.port)
    submit('error', 'forward_error', str(pool['forward_error']),
           self.server, self.port)
    submit('count', 'fragments', str(pool['fragments']),
           self.server, self.port)


  def send_stats_to_collectd(self, content):
    """
      Parse stats content string, send values to collectd.

      :param content: stats string, in json format.
    """
    stats = json.loads(content)

    for k in sorted(stats.keys()):
      try:
        # Only look into kv-pools, skip high-level summary stats.
        v = stats[k]
        if type(v) is dict:
          # Now 'k' is pool name, 'v' is object representing the pool.
          parse_pool(k, v)

          # Look into each server in the pool.
          for bk in v.keys():
            # TODO: summarize counts / errors of all servers.
            if type(v[bk]) is dict:
              # Now 'bk' is backend server name
              parse_server(bk, v[bk])
      except:
        pass


  def read_proxy_stats(self):
    """
      Get actual data from proxy, pass them to Collectd.

    """
    conn = socket.create_connection((self.server, self.port))
    buf = True
    content = ''
    while buf:
      buf = conn.recv(4096)
      content += buf
    conn.close()

    print "content : %s" % (content)
    self.send_stats_to_collectd(content);


proxy = KVProxyPlugin()
collectd.register_config(proxy.config)
collectd.register_read(proxy.read_proxy_stats, proxy.interval)
