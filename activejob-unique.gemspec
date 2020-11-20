require File.expand_path('../lib/active_job/unique/version', __FILE__)

Gem::Specification.new do |s|
    s.name = 'activejob-unique'
    s.version     = ActiveJob::Unique::VERSION.to_s
    s.date        = '2020-07-26'
    s.summary     = 'ActiveJob unique jobs'
    s.description = 'ActiveJob uniqueness job adpater'
    s.authors     = ['josh.c']
    s.email       = 'junyu.live@gmail.com'
    s.homepage    = 'https://github.com/Modernech/activejob-unique'
    s.license     = 'MIT'

    s.require_paths = ['lib']
    s.files         = `git ls-files`.split("\n")

    s.add_dependency 'activejob'
    s.add_dependency 'activesupport'
    s.add_dependency 'sidekiq', '>= 4.2', '< 6'
end
