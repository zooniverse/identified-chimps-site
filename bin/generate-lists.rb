require 'aws-sdk'
require 'json'
require 'mongo'
require 'yaml'
include Mongo

$species_to_track = %w(chimpanzee gorilla other-(primate))
$gorilla_sites = %w(restless-star)
$moderators = %w(MimiA NuriaM maureenmccarthy northernlimitptv PauDG akalan Silke_Atmaca Quia ksigler yshish AnLand jwidness)

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

def is_verified(subject)
  subject['metadata']['counters'] != nil and subject['classification_count'] > 0
end

def species_key(species)
  "#{ species.gsub(/[\(\)]/,'') }"
end

def tags_for(db, zooniverse_id)
  db['discussions'].aggregate([
      {:$match => {'focus._id' => zooniverse_id, 'comments.$.tags' => { '$ne' => [] }}},
      {:$project => {'_id' => 0, 'comments' => 1}},
      {:$unwind => '$comments'},
      {:$project => {'tags' => '$comments.tags'}},
      {:$unwind => '$tags'},
      {:$group => {'_id' => '$tags', 'count' => { :$sum => 1 }}},
      {:$sort => {count: -1}}
    ])
end

def mod_tags_for(db, zooniverse_id)
  db['discussions'].aggregate([
      {:$match => {'focus._id' => zooniverse_id, 'comments.$.tags' => { :$ne => [] }}},
      {:$project => {'_id' => 0, 'comments' => 1}},
      {:$unwind => '$comments'},
      {:$match => {'comments.user_name' => {:$in => $moderators}}},
      {:$project => {'tags' => '$comments.tags'}},
      {:$unwind => '$tags'},
      {:$group => {'_id' => '$tags', 'count' => { :$sum => 1 }}},
      {:$sort => {'count' => -1}}
    ])
end

def check_list_for_string(lst, str)
  lst.each do |s|
    if s[str] != nil
      return true
    end
  end
  return false
end

def add_to_hash(db, hash, document, species)
  group_id = document['group']['zooniverse_id']
  group_name = document['group']['name']

  hash[group_id] ||= {}

  hash[group_id]['id'] ||= group_id
  hash[group_id]['name'] ||= group_name

  hash[group_id][species_key(species)] ||= []
  hash[group_id][species_key(species)] << {
    zooniverse_id: document['zooniverse_id'],
    tags: tags_for(db, document['zooniverse_id']).collect{ |tag| tag['_id'] },
    file: document['metadata']['file'],
    start_time: document['metadata']['start_time']
  }
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

sites_to_track = db['chimp_groups'].aggregate([
      {:$match => {'state' => { :$in => ['complete','active'] }}}
      ]).collect{ |site| site['name'] }
sites_to_track = sites_to_track - ['frosty-sky-3']

aggregate_species_hash = {}
db['chimp_subjects'].find({}, read: :secondary).each do |document|
  site_name = document['group']['name']
  classification_count = document['classification_count']
  if sites_to_track.include?(site_name)
    mod_tags = mod_tags_for(db, document['zooniverse_id']).collect{ |tag| tag['_id'] }
    if (check_list_for_string(mod_tags, 'chimp') or mod_tags.include?('gorilla'))
      if (check_list_for_string(mod_tags, 'chimp')
        add_to_hash(db, aggregate_species_hash, document, 'chimpanzee')
      end
      if $gorilla_sites.include?(site_name) and mod_tags.include?('gorilla')
        add_to_hash(db, aggregate_species_hash, document, 'gorilla')
      end
    else
      all_tags = tags_for(db, document['zooniverse_id']).collect{ |tag| tag['_id'] }
      chimp_count = species_count(document, 'chimpanzee')
      gorilla_count = species_count(document, 'gorilla')
      primate_count = species_count(document, 'other-(primate)')

      if !mod_tags.include?('omit') and (check_list_for_string(all_tags, 'chimp') or (is_verified(document) and chimp_count >= 2 and primate_count <= 2 * chimp_count))
       add_to_hash(db, aggregate_species_hash, document, 'chimpanzee')
      end

      if $gorilla_sites.include?(site_name) and !mod_tags.include?('omit') and (check_list_for_string(all_tags, 'gorilla') or (is_verified(document) and gorilla_count >= 2 and primate_count <= 2 * gorilla_count))
       add_to_hash(db, aggregate_species_hash, document, 'gorilla')
      end
    end

    ($species_to_track - ['chimpanzee', 'gorilla']).each do |species|
      count = species_count(document, species)
      blanks = blank_count(document)
      if document['state'] == 'complete' and is_verified(document) and count >= 2 and count >= ((classification_count - blanks) / 2)
        add_to_hash(db, aggregate_species_hash, document, species)
      end
    end
  end
end

puts "Query took #{ Time.now - now }"

sorted_groups = aggregate_species_hash.values.sort_by { |group| group['id'] }
sorted_groups.each do |group|
  $species_to_track.each do |species|
    if group.has_key?(species_key(species))
      group[species_key(species)].sort!{|x, y| (x[:file] <=> y[:file]).nonzero? || x[:start_time] <=> y[:start_time]}
      group[species_key(species)].map{|x| x.delete(:file)}
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
