MRuby::Gem::Specification.new('mruby-memcached') do |spec|
  spec.license = 'MIT'
  spec.authors = 'MATSUMOTO Ryosuke'
  spec.version = '0.0.1'
  spec.linker.libraries << "memcached"
end
