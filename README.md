# mruby-memcached   [![Build Status](https://travis-ci.org/matsumoto-r/mruby-memcached.png?branch=master)](https://travis-ci.org/matsumoto-r/mruby-memcached)
[libmemcached](http://libmemcached.org/libMemcached.html) bindings by mruby.
## install by mrbgems 
- add conf.gem line to `build_config.rb` 

```ruby
MRuby::Build.new do |conf|

    # ... (snip) ...

    conf.gem :git => 'https://github.com/matsumoto-r/mruby-memcached.git'
end
```
## example 
```ruby
> m = Memcached.new "127.0.0.1", 11211
> m.set "hoge", "1"
 => 0
# defaule expire time is 600 sec
# m.set :foo, 10, 600
# if you want expire time 1000 sec,
# m.set :foo, 10, 1000
> m.set :foo, 10
 => 0
> m.get "hoge"
 => "1"
> m.get :hoge
 => "1"
> m.get "foo"
 => "10"
> m.close
```
#### Replicas
```ruby
m = Memcached.new "127.0.0.1", 11211
m.server_add "127.0.0.1", 11212

m.behavior_set Memcached::MEMCACHED_BEHAVIOR_BINARY_PROTOCOL
m.behavior_set Memcached::MEMCACHED_BEHAVIOR_NUMBER_OF_REPLICAS

m.set :hoge, 5
p m.get :hoge # => 5

m.close
```
memcat 
```
$ memcat --servers=127.0.0.1:11211 hoge
5

$ memcat --servers=127.0.0.1:11212 hoge
5
```


## License
under the MIT License:
- see LICENSE file
