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

## License
under the MIT License:
- see LICENSE file
