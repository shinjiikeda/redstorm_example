redstorm example
=====
setup
   
    $ rvm install jruby
    $ rvm use jruby
    $ bundle install --binstubs --path vendor/bundle

run (local mode)

    $ ./bin/redstorm install
    $ ./bin/redstorm bundle
    $ ./bin/redstorm local test.rb 

