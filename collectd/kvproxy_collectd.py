#! /usr/bin/env python


import collectd
import json
import socket
import sys
from random import randint

PLUGIN_NAME = 'distkvproxy'
VERBOSE_LOGGING = False

def log_verbose(msg):
  if VERBOSE_LOGGING:
    collectd.info('{} [verbose]: {}'.format(PLUGIN_NAME, msg))


class KVProxyPlugin(object):
  """
    This class collects KV proxy stats info, and passes to Collectd.
  """

  def __init__(self, ip=None, port=None):
    self.ips = []
    self.ports = []

    if ip and port:
      self.ips.append(ip)
      self.ports.append(port)
 
    # default interval is 20 seconds.
    self.interval = 20
    self.plugin_name = PLUGIN_NAME
    self.test = False


  def config(self, conf):
    """
      Config callback.

      Collectd will call this method to init the plugin.

      :param conf: a Config object containing all info representing the
                   config tree.
      example config section:

      <Module redisproxy_collectd>
        proxy    "ip1:port1" "ip1:port2" "ip1:port3"
        interval 20
        verbose  true/false
      </Module>

      For how to interpret config object, see here:
      https://collectd.org/documentation/manpages/collectd-python.5.shtml
    """
    for node in conf.children:
      key = node.key.lower()

      collectd.info('config: key: {}, value: {}'.format(key, node.values))
      if key == 'proxy':
        for s in node.values:
          tp = s.split(':')
          if len(tp) == 2:
            self.ips.append(tp[0])
            self.ports.append(int(tp[1]))
          else:
            collectd.warning('KVProxyPlugin: invalid proxy address %s' % s)
      elif key == 'interval':
        self.interval = float(node.values[0])
      elif key == 'verbose':
        global VERBOSE_LOGGING
        if node.values[0]:
        #in ['true', 'True']:
          VERBOSE_LOGGING = True
      elif key == 'test':
        # if we are in test mode
        self.test = node.values[0]
      else:
        collectd.warning('KVProxyPlugin: Unkown configuration key %s'
                         % node.key)
    collectd.info('have inited plugin {}'.format(PLUGIN_NAME))


  def submit(self, type, type_instance, value, server, port):
    """
      dispatch a msg to collectd.
    """
    plugin_instance = '{}_{}'.format(server, port)

    v = collectd.Values()
    v.plugin = self.plugin_name
    v.plugin_instance = plugin_instance
    v.type = type
    v.type_instance = type_instance
    if self.test:
      value = randint(50, 100)
    v.values = [value, ]

    try:
      v.dispatch()
    except Exception as e:
      collectd.info('failed to dispatch data {}:{}: {}'.format(
                    type_instance, value, e))


  def parse_server(self, sname, server, ip, port):
    """
      Parse proxy stats info about a server, then send to collectd

      :param sname:  backend server name
      :param server: json obj representing a server stats.
      :param ip:      proxy ip address
      :param port:    proxy port
    """
    self.submit('tcp_connections',
                'server_connections_%s' % sname,
                str(server['server_connections']),
                ip, port)
    self.submit('counter',
                'server_eof_%s' % sname,
                str(server['server_eof']),
                ip, port)
    self.submit('counter',
                'server_err_%s' % sname,
                str(server['server_err']),
                ip, port)
    self.submit('counter',
                'requests_%s' % sname,
                str(server['requests']),
                ip, port)
    self.submit('counter',
                'request_bytes_%s' % sname,
                str(server['request_bytes']),
                ip, port)
    self.submit('counter',
                'responses_%s' % sname,
                str(server['responses']),
                ip, port)
    self.submit('counter',
                'response_bytes_%s' % sname,
                str(server['response_bytes']),
                ip, port)
    self.submit('gauge',
                'in_queue_%s' % sname,
                str(server['in_queue']),
                ip, port)
    self.submit('gauge',
                'in_queue_bytes_%s' % sname,
                str(server['in_queue_bytes']),
                ip, port)
    self.submit('gauge',
                'out_queue_%s' % sname,
                str(server['out_queue']),
                ip, port)
    self.submit('gauge',
                'out_queue_bytes_%s' % sname,
                str(server['out_queue_bytes']),
                ip, port)


  def parse_pool(self, pname, pool, ip, port):
    """
      Parse proxy stats info about a KV pool, then send to collectd

      :param pname: pool name
      :param pool:  json obj representing pool stats.
      :param ip:      proxy ip address
      :param port:    proxy port
    """

    # First, record top summaries for this pool.
    self.submit('tcp_connections', 'client_connections', str(pool['client_connections']),
                ip, port)
    self.submit('counter', 'client_err', str(pool['client_err']),
                ip, port)
    self.submit('counter', 'client_eof', str(pool['client_eof']),
                ip, port)
    self.submit('counter', 'server_ejects', str(pool['server_ejects']),
                ip, port)
    self.submit('counter', 'forward_error', str(pool['forward_error']),
                ip, port)
    self.submit('counter', 'fragments', str(pool['fragments']),
                ip, port)


  def send_stats_to_collectd(self, content, ip, port):
    """
      Parse stats content string, send values to collectd.

      :param content: stats string, in json format.
      :param ip:      proxy ip address
      :param port:    proxy port
    """
    stats = json.loads(content)

    for k in sorted(stats.keys()):
      # Only look into kv-pools, skip high-level summary stats.
      v = stats[k]
      if type(v) is dict:
        # Now 'k' is pool name, 'v' is object representing the pool.
        self.parse_pool(k, v, ip, port)
        continue

        # Look into each server in the pool.
        for bk in v.keys():
          # TODO: summarize counts / errors of all servers.
          if type(v[bk]) is dict:
            # Now 'bk' is backend server name
            self.parse_server(bk, v[bk], ip, port)


  def read_proxy_stats(self):
    """
      Get actual data from proxy, pass them to Collectd.

    """
    for i in range(len(self.ips)):
      ip = self.ips[i]
      port = self.ports[i]
      try:
        log_verbose('will read stats at {}:{}'.format(ip, port))
        conn = socket.create_connection((ip, port))
        buf = True
        content = ''
        while buf:
          buf = conn.recv(4096)
          if buf:
            content += buf
        conn.close()
        self.send_stats_to_collectd(content, ip, port);
      except Exception as e:
        log_verbose('Error:: {}'.format(e))


def main():
  ip = '192.168.0.38'
  port = 22222
  proxy = KVProxyPlugin(ip, port)
  proxy.read_proxy_stats()

if __name__ == '__main__':
  main()
  sys.exit(0)

proxy = KVProxyPlugin()
collectd.register_config(proxy.config)
collectd.register_read(proxy.read_proxy_stats, proxy.interval)
