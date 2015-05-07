config = 
  data_file_location: 'http://www.chimpandsee.org/identified-chimps/identified-chimps.json'
  is_scientist: 'no' # This doesn't do anything yet.

pairs = location.search.slice(1).split(',')
for pair in pairs
  for key, value of pair
    continue unless config.hasOwnProperty(key)
    config[key] = value

module.exports = config
