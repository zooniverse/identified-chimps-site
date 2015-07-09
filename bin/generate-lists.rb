require 'aws-sdk'
require 'json'
require 'mongo'
require 'yaml'
include Mongo

species_to_track = %w(chimpanzee other-(primate))

def species_count(subject, species)
  subject['metadata']['counters'][species] || 0
end

def species_key(species)
  "#{ species.gsub(/[\(\)]/,'') }"
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

now = Time.now

# Actual work
puts "Querying mongo..."

aggregate_species_hash = {}
db['chimp_subjects'].find({ state: 'complete' }, read: :secondary).each do |document|
  group_id = document['group']['zooniverse_id']
  group_name = document['group']['name']
  classification_count = document['classification_count']

  aggregate_species_hash[group_id] ||= {}

  aggregate_species_hash[group_id]['id'] ||= group_id
  aggregate_species_hash[group_id]['name'] ||= group_name

  species_to_track.each do |species|
    if species_count(document, species) >= (classification_count / 2)
      aggregate_species_hash[group_id][species_key(species)] ||= []
      aggregate_species_hash[group_id][species_key(species)] << document['zooniverse_id']
    end
  end
end

puts "Query took #{ Time.now - now }"

sorted_groups = aggregate_species_hash.values.sort_by { |group| group['id'] }
sorted_groups.each do |group|
  species_to_track.each do |species|
    if group.has_key?(species_key(species))
      group[species_key(species)].sort!
    end
  end
end

puts "Writing data file..."
s3.put_object(
  body: sorted_groups.to_json,
  bucket: config['aws']['bucket'],
  key: config['app']['data_file_location'],
  cache_control: 'no-cache, must-revalidate',
  content_type: 'application/json',
  acl: 'public-read'
)

puts "Done!"
