config =
  data_file_location: 'https://www.chimpandsee.org/identified-chimps/identified-species.json'
  is_scientist: 'no' # This doesn't do anything yet.
  speciesToTrack: ['chimpanzee', 'other-primate']

pairs = location.search.slice(1).split(',')
for pair in pairs
  for key, value of pair
    continue unless config.hasOwnProperty(key)
    config[key] = value

module.exports = config
