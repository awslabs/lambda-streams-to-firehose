#!/usr/bin/env ruby

require 'yaml'
require 'json'
require 'optparse'
require 'ostruct'
require 'fileutils'

#quit unless our script gets two command line arguments
unless ARGV.length == 3
  puts "Dude, not the right number of arguments."
  puts "Usage: ruby YJ_Convert.rb [-j][-y] json_file.json yaml_file.yaml\n"
  exit
end

$json_file = ARGV[1]
$yaml_file = ARGV[2]

options = OpenStruct.new
OptionParser.new do |opt|
  opt.on('-j', '--json', 'Convert to JSON') { |o| options.json = o }
  opt.on('-y', '--yaml', 'Convert to YAML') { |o| options.yaml = o }
end.parse!

case
  when options.yaml == true
    y_file = File.open("#{$yaml_file}", 'a')
    y_file.write(YAML.dump(JSON.parse(IO.read($json_file))))
    y_file.close
    puts "Converted to YAML. Output file is #{$yaml_file}"

  when options.json == true
    j_file = YAML.load_file(File.open("#{$yaml_file}", 'r'))
    File.write "#{$json_file}", JSON.pretty_generate(j_file)
    puts "Converted to JSON. Output file is #{$json_file}"
end
