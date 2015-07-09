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

  accordianContainer = document.querySelector('#species-accordians-container')
  config['speciesToTrack'].forEach (species) ->
    table = require('./views/accordian')({ data, species })
    accordianContainer.insertAdjacentHTML 'afterbegin', table

  setTimeout ->
    $('.accordian').collapse()
