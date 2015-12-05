#! /usr/bin/env python


import collectd
import socket
import json
from datetime import timedelta

plugin_name = 'KVProxy'

class KVProxyPlugin(object):
  """
    This class collects KV proxy stats info, and passes to Collectd.
  """

  def __init__(self):
    self.server = '127.0.0.1'
    self.port = '22222'
    # default interval is 20 seconds.
    self.interval = 20

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


  def submit(self, type, instance, value, server=None):
    if server:
      plugin_instance = '%s-%s' % (self.port, server)
    else:
      plugin_instance = str(self.port)

    v = collectd.Values()
    v.plugin = self.plugin_name
    v.plugin_instance = plugin_instance
    v.type = type
    v.type_instance = instance
    v.values = [value, ]
    v.dispatch()


  def read_proxy_stats(self):
    """
      Get actual data from proxy, pass them to Collectd.

    """
    conn = socket.create_connection((self.server, self.port))
    buf = True
    content = ''
    while buf:
      buf = conn.recv(1024)
      content += buf
    conn.close()

    print "content : %s" % (content)
    self.send_stats_to_collectd(content);


  def parse_server(self, sname, server):
    """
      Parse stats info about a server, send to collectd
      
      :param sname:  server name
      :param server: json obj representing a server stats.
    """
    total_req = 0
    total_req_bytes = 0
    total_rsp = 0
    total_rsp_bytes = 0

    val = collectd.Values()
    val.plugin = plugin_name;
    val.plugin_instance = sname
    val.type = 'counter'
    val.type_instance = 'client_connections'
    val.values = [str(pool['client_connections'])]
    val.dispatch()


  def parse_pool(self, pname, pool):
    """
      Parse stats info about a KV pool, send to collectd
      
      :param pname: pool name
      :param pool:  json obj representing pool stats.
    """

    # First, show top summary for this pool.
    val = collectd.Values()
    val.plugin = plugin_name;
    val.plugin_instance = pname;
    val.type = 'connections'
    val.type_instance = 'client_connections'
    val.values = [str(pool['client_connections'])]
    val.dispatch()

    val = collectd.Values()
    val.plugin = plugin_name;
    val.plugin_instance = pname;
    val.type = 'error'
    val.type_instance = 'client_err'
    val.values = [str(pool['client_err'])]
    val.dispatch()

    val = collectd.Values()
    val.plugin = plugin_name;
    val.plugin_instance = pname;
    val.type = 'error'
    val.type_instance = 'server_ejects'
    val.values = [str(pool['server_ejects'])]
    val.dispatch()

    val = collectd.Values()
    val.plugin = plugin_name;
    val.plugin_instance = pname;
    val.type = 'error'
    val.type_instance = 'forward_error'
    val.values = [str(pool['forward_error'])]
    val.dispatch()

    # Now, look into each server in the pool.
    for k, v in pool.iteritems():
      if type(v) not dict:
        pass
      # now we know "k" is server name, v is dict.
      self.parse_server(k, v)


  def send_stats_to_collectd(self, content):
    """
      Parse stats content string, send values to collectd.

      :param content: stats string, in json format.
    """

    stats = json.loads(content)

    for k in sorted(stats.keys()):
      try:
        v = stats[k]
        # Only look into kv-pools, skip high-level summary stats.
        v['server_ejects']
        
        # now 'k' is pool name

        #  dispatch([type][, values][, plugin_instance][, type_instance][, plugin][, host][, time][, interval]) -> None.
        metric = collectd.Values()
        metric.plugin = 'twemproxy-%s' % k
        metric.type_instance = 'client_connections'
        #  metric.plugin_instance = 'client_connections'
        metric.type = 'tcp_connections'
        metric.values = [str(v['client_connections'])]
        metric.dispatch()

        metric = collectd.Values()
        metric.plugin = 'twemproxy-%s' % k
        metric.type_instance = 'client_eof'
        metric.type = 'derive'
        metric.values = [str(v['client_eof'])]
        metric.dispatch()

        metric = collectd.Values()
        metric.plugin = 'twemproxy-%s' % k
        metric.type_instance = 'forward_error'
        metric.type = 'derive'
        metric.values = [str(v['forward_error'])]
        metric.dispatch()

        metric = collectd.Values()
        metric.plugin = 'twemproxy-%s' % k
        metric.type_instance = 'client_err'
        metric.type = 'derive'
        metric.values = [str(v['client_err'])]
        metric.dispatch()

        metric = collectd.Values()
        metric.plugin = 'twemproxy-%s' % k
        metric.type_instance = 'fragments'
        metric.type = 'derive'
        metric.values = [str(v['fragments'])]
        metric.dispatch()

        metric = collectd.Values()
        metric.plugin = 'twemproxy-%s' % k
        metric.type_instance = 'server_ejects'
        metric.type = 'derive'
        metric.values = [str(v['server_ejects'])]
        metric.dispatch()

        for bk in v.keys():
          if type(v[bk]) is dict:
            metric = collectd.Values()
            metric.plugin = 'twemproxy-%s' % k
            metric.type_instance = '%s-server_eof' % bk
            metric.type = 'derive'
            metric.values = [str(v[bk]['server_eof'])]
            metric.dispatch()

            metric = collectd.Values()
            metric.plugin = 'twemproxy-%s' % k
            metric.type_instance = '%s-server_err' % bk
            metric.type = 'derive'
            metric.values = [str(v[bk]['server_err'])]
            metric.dispatch()

            metric = collectd.Values()
            metric.plugin = 'twemproxy-%s' % k
            metric.type_instance = '%s-server_connections' % bk
            metric.type = 'gauge'
            metric.values = [str(v[bk]['server_connections'])]
            metric.dispatch()

            metric = collectd.Values()
            metric.plugin = 'twemproxy-%s' % k
            metric.type_instance = '%s-server_timedout' % bk
            metric.type = 'derive'
            metric.values = [str(v[bk]['server_timedout'])]
            metric.dispatch()

            metric = collectd.Values()
            metric.plugin = 'twemproxy-%s' % k
            metric.type_instance = '%s-responses' % bk
            metric.type = 'counter'
            metric.values = [str(v[bk]['responses'])]
            metric.dispatch()

            metric = collectd.Values()
            metric.plugin = 'twemproxy-%s' % k
            metric.type_instance = '%s-response_bytes' % bk
            metric.type = 'total_bytes'
            metric.values = [str(v[bk]['response_bytes'])]
            metric.dispatch()

            metric = collectd.Values()
            metric.plugin = 'twemproxy-%s' % k
            metric.type_instance = '%s-in_queue_bytes' % bk
            metric.type = 'gauge'
            metric.values = [str(v[bk]['in_queue_bytes'])]
            metric.dispatch()

            metric = collectd.Values()
            metric.plugin = 'twemproxy-%s' % k
            metric.type_instance = '%s-out_queue_bytes' % bk
            metric.type = 'gauge'
            metric.values = [str(v[bk]['out_queue_bytes'])]
            metric.dispatch()

            metric = collectd.Values()
            metric.plugin = 'twemproxy-%s' % k
            metric.type_instance = '%s-request_bytes' % bk
            metric.type = 'derive'
            metric.values = [str(v[bk]['request_bytes'])]
            metric.dispatch()

            metric = collectd.Values()
            metric.plugin = 'twemproxy-%s' % k
            metric.type_instance = '%s-requests' % bk
            metric.type = 'derive'
            metric.values = [str(v[bk]['requests'])]
            metric.dispatch()

            metric = collectd.Values()
            metric.plugin = 'twemproxy-%s' % k
            metric.type_instance = '%s-in_queue' % bk
            metric.type = 'gauge'
            metric.values = [str(v[bk]['in_queue'])]
            metric.dispatch()

            metric = collectd.Values()
            metric.plugin = 'twemproxy-%s' % k
            metric.type_instance = '%s-out_queue' % bk
            metric.type = 'gauge'
            metric.values = [str(v[bk]['out_queue'])]
            metric.dispatch()
      except:
        pass


proxy = KVProxyPlugin()
collectd.register_config(proxy.config)
collectd.register_read(proxy.read_proxy_stats, proxy.interval)
