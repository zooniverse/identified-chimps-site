require 'aws-sdk'
require 'json'
require 'mongo'
require 'yaml'
include Mongo

species_to_track = %w(chimpanzee other-(primate))

def species_count(subject, species)
  if subject['metadata']['counters'] != nil
    subject['metadata']['counters'][species] || 0
  else
    nil
  end
end

def blank_count(subject)
  if subject['metadata']['counters'] != nil
    subject['metadata']['counters']['blank'] || 0
  else
    nil
  end
end

def species_key(species)
  "#{ species.gsub(/[\(\)]/,'') }"
end

def tags_for(db, zooniverse_id)
  db['discussions'].aggregate([
      {'$match': {'focus._id': zooniverse_id, 'comments.$.tags': { '$ne': [] }}},
      {'$project': {_id: 0, comments: 1}},
      {'$unwind': '$comments'},
      {'$project': {tags: '$comments.tags'}},
      {'$unwind': '$tags'},
      {'$group': {_id: '$tags', count: { '$sum': 1 }}},
      {'$sort': {count: -1}}
    ])
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
db['chimp_subjects'].find({ state: 'complete'}, read: :secondary).each do |document|
  group_id = document['group']['zooniverse_id']
  group_name = document['group']['name']
  classification_count = document['classification_count']

  aggregate_species_hash[group_id] ||= {}

  aggregate_species_hash[group_id]['id'] ||= group_id
  aggregate_species_hash[group_id]['name'] ||= group_name

  species_to_track.each do |species|
    count = species_count(document, species)
    blanks = blank_count(document)
    if count != nil and classification_count > 0 and count >= ((classification_count - blanks) / 2)
      aggregate_species_hash[group_id][species_key(species)] ||= []
      aggregate_species_hash[group_id][species_key(species)] << {
        zooniverse_id: document['zooniverse_id'],
        tags: tags_for(db, document['zooniverse_id']).collect{ |tag| tag['_id'] },
        start_time: document['metadata']['start_time']
      }
    end
  end
end

puts "Query took #{ Time.now - now }"

sorted_groups = aggregate_species_hash.values.sort_by { |group| group['id'] }
sorted_groups.each do |group|
  species_to_track.each do |species|
    if group.has_key?(species_key(species))
      group[species_key(species)].sort!{|x, y| x['zooniverse_id'] <=> y['zooniverse_id']}
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
