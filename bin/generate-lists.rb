require 'aws-sdk'
require 'json'
require 'mongo'
require 'yaml'
include Mongo

def chimp_identified?(subject)
  total = subject['classification_count']
  chimp_votes = subject['metadata']['counters']['chimpanzee'] || 0

  return chimp_votes > (total / 2)
end

config_file_path = ARGV[0] || File.dirname(__FILE__) + '/config.yml'
config = YAML.load File.read config_file_path

# AWS
Aws.config.update({
  region: config['aws']['region'],
  credentials: Aws::Credentials.new(config['aws']['key'], config['aws']['secret'])
})

s3 = Aws::S3::Client.new

# Mongo
client = MongoClient.new(config['mongo']['host'], config['mongo']['port'])
db = client[config['mongo']['database']]
auth = db.authenticate(config['mongo']['username'], config['mongo']['password'])

unless auth
  puts "Failed to authenticate. Exiting..."
  exit
end

# Actual work
puts "Querying mongo..."
aggregate_chimps_hash = {}
db['chimp_subjects'].find({ state: 'complete'}, read: :secondary).each do |document|
  next unless chimp_identified?(document)

  group_id = document['group']['zooniverse_id']
  group_name = document['group']['name']

  aggregate_chimps_hash[group_id] ||= {}

  aggregate_chimps_hash[group_id]['id'] ||= group_id
  aggregate_chimps_hash[group_id]['name'] ||= group_name
  aggregate_chimps_hash[group_id]['identified_subjects'] ||= []

  aggregate_chimps_hash[group_id]['identified_subjects'] << document['zooniverse_id']
end
identified_chimps = aggregate_chimps_hash.values.sort_by { |group| group['id'] }
identified_chimps.each { |group| group['identified_subjects'].sort! }

puts "Writing data file..."
s3.put_object(
  body: identified_chimps.to_json,
  bucket: config['aws']['bucket'],
  key: config['app']['data_file_location'],
  cache_control: 'no-cache, must-revalidate',
  content_type: 'application/json',
  acl: 'public-read'
)

puts "Done!"
