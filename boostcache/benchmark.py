#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys, select, time, os, re
import datetime
import optparse
import string
import random
sys.path.append('../')
import redis

KB = 1024

def init_parser(options):
           options.add_option("-n", "--value_number",
                              action = "store",
                              default = 200,
                              dest = "value_number",
                              help = "Number of values will be generated to benchmark boostcache for one client. Default is 200.")
           options.add_option("-c", "--clients_number",
                              action = "store",
                              default = 50,
                              dest = "clients_number",
                              help = "Number of parallel clients. Default is 50.")
           options.add_option("-s", "--server_socket",
                              action = "store",
                              dest = "server_socket",
                              help = "Path to the server socket.")
           options.add_option("-a", "--address",
                              action = "store",
                              default = "localhost:9876",
                              dest = "address",
                              help = "Boostcache server address. Default is localhost:9876.")
           options.add_option("-l", "--keys_length",
                              action = "store",
                              default = "1K",
                              dest = "length",
                              help = "Keys length. 1M = 1024K = 1048576 symbols. E. g. -l 5K, for keys length 5120 symbols. Default is 1K.")

def check_options(options):
        try:
            options.value_number = int(options.value_number)
            options.clients_number= int(options.clients_number)
        except:
            print "Invalid arguments. Try -h to see help."
            raise SystemExit(1)
        if options.server_socket:
            if not os.path.exists(options.server_socket):
                print "Could not find server socket", options.server_socket
                raise SystemExit(1)
        else:
            if not re.match("^([a-zA-Z]+|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})):[0-9]+$", options.address):
                print "Invalid server address:", options.address
                raise SystemExit(1)
        if re.match("[0-9]+[mMkK]{0,1}$", options.length):
            if options.length[::-1][0] == 'm' or options.length[::-1][0] == 'M':
                options.length = int(options.length[:-1]) * KB * KB
            elif options.length[::-1][0] == 'k' or options.length[::-1][0] == 'K':
                options.length = int(options.length[:-1]) * KB
            else:
                options.length = int(options.length)
        else:
            print "Invalid keys length:", options.length
            raise SystemExit(1)


def rand_word(length):
    return ''.join(random.choice(string.letters) for x in range(length))

def create_dict(number, length):
    keys_dict = {}
    for i in range(number):
        keys_dict.update({rand_word(length) : rand_word(length)})
    return keys_dict


class boostcache_benchmark():
    def __init__(self, number, clients_number, keys_dict, socket, host = 'localhost', port = 9876):
        self.keys_number = number
        self.clients_number = clients_number
        self.keys_dict = keys_dict
        self.test_number = number
        self.clients_dict = {}
        self.socket = socket
        self.host = host
        self.port = port
        self.epoll = select.epoll()
        self.create_connections()

    def create_connections(self):
        for i in range(self.clients_number):
            if self.socket:
                client = redis.Boostcache(unix_socket_path = self.socket)
            else:
                client = redis.Boostcache(host = 'localhost', port = 9876, db = 0)
            client.ping()
            self.clients_dict[client.connection._sock.fileno()] = client
            self.epoll.register(client.connection._sock.fileno(), select.EPOLLOUT)

    def run(self, function):
        one_percent = (self.keys_number * self.clients_number) / 100.0
        total_time = 0.0
        tests = 0
        for key, value in self.keys_dict.items():
            events = self.epoll.poll(1)
            for fileno, event in events:
                total_time += function(self.clients_dict[fileno], key, value)
                tests += 1
                if tests % (one_percent * 10) == 0:
                    print "Done:", (tests / one_percent), "% -- ", total_time, "seconds"
        print "Total: ", total_time, "seconds. Number of tests:", tests
        if total_time:
            print "%8.3f requests per second" % (tests / total_time)

    def hset_benchmark(self, client, key, value):
        start_time = time.clock()
        client.hset(key, value)
        return time.clock() - start_time

    def hget_benchmark(self, client, key, value):
        start_time = time.clock()
        client.hget(key)
        return time.clock() - start_time

    def hdel_benchmark(self, client, key, value):
        start_time = time.clock()
        client.hdel(key)
        return time.clock() - start_time

    def atset_benchmark(self, client, key, value):
        start_time = time.clock()
        client.atset(key, value)
        return time.clock() - start_time

    def atget_benchmark(self, client, key, value):
        start_time = time.clock()
        client.atget(key)
        return time.clock() - start_time

    def atdel_benchmark(self, client, key, value):
        start_time = time.clock()
        client.atdel(key)
        return time.clock() - start_time

    def run_hbenchmark(self):
        print "HSET BENCHMARK:"
        self.run(self.hset_benchmark)
        print "HGET BENCHMARK:"
        self.run(self.hget_benchmark)
        print "HDEL BENCHMARK:"
        self.run(self.hdel_benchmark)
        print "ATSET BENCHMARK:"
        self.run(self.atset_benchmark)
        print "ATGET BENCHMARK:"
        self.run(self.atget_benchmark)
        print "ATDEL BENCHMARK:"
        self.run(self.atdel_benchmark)

if __name__ == "__main__":
    parser = optparse.OptionParser()
    init_parser(parser)
    (options, args) = parser.parse_args()
    check_options(options)
    keys_dict = create_dict(options.value_number, options.length)
    benchmark = boostcache_benchmark(options.value_number,
                                     options.clients_number,
                                     keys_dict,
                                     options.server_socket,
                                     host = options.address.split(':')[0],
                                     port = options.address.split(':')[1])
    benchmark.run_hbenchmark()
