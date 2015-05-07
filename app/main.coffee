require 'whatwg-fetch'
$ = window.jQuery

config = require './lib/config'
fetch(config['data_file_location']).then((res) -> res.json()).then (json) ->
  # Caching issue. Can be removed in time.
  unless Array.isArray json
    data = []
    for key, value of json
      data.push value
  else
    data = json

  chimpsTable = require('./views/chimps-accordian')({ chimps: data })

  chimpsDataContainer = document.querySelector('#chimps-accordian-container')
  chimpsDataContainer.innerHTML = chimpsTable
  setTimeout -> $(chimpsDataContainer).collapse()
