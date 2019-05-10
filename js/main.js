/// <reference types="aws-sdk" />
var zoneDistHeaders = [];
var landUseHeaders = [];

function addHeadersToZoningDropdown() {
  for (var header in zoneDistHeaders) {
    label = zoneDistHeaders[header]
    $('#dropdown-zoneDist-menu').append(
      $('<div class="item"></div>').val(header).html(label)
    )
  }
}

function compileZoningDistrictLabels(queryData) {
  for (var i=1; i < queryData["ResultSet"]["Rows"].length; i++) {
    label = queryData["ResultSet"]["Rows"][i]["Data"][0]["VarCharValue"]
    if (label != "zonedist1" && label != "") {
      zoneDistHeaders.push(label);
    }
  }
  zoneDistHeaders.sort();
  addHeadersToZoningDropdown();
}

function compileLandUseLabels(queryData) {
  for (var i=1; i < queryData["ResultSet"]["Rows"].length; i++) {
    label = queryData["ResultSet"]["Rows"][i]["Data"][0]["VarCharValue"]
    if (label != "landuse" && label != "") {
      landUseHeaders.push(label);
    }
  }
  landUseHeaders.sort();
}

$(document).ready(function() {

   $("#dropdown-zoneDist").dropdown();

   // Populate dropdown with all zoning districts
   sql_command = 'SELECT DISTINCT zonedist1 FROM "pluto_db"."pluto_latlonincluded" limit 300';
   submitAthenaQuery(sql_command, "FIND ZONING LABELS");
})

var activeLayersOnMap = []

mapboxgl.accessToken ='pk.eyJ1IjoiZGNoYXJ2ZXkiLCJhIjoiY2plZGJxZTRpMHRuMzJ3b2QxMjZ5YWJ5MyJ9.jvqvM0KoLvKvhglXy1cKiQ';
var map = new mapboxgl.Map({
    container: 'map', // container id
    style: 'mapbox://styles/mapbox/streets-v11', // stylesheet location
    center: [-73.93, 40.74], // starting position [lng, lat]
    zoom: 7 // starting zoom
});

map.on('load', function() {

  //   new mapboxgl.Popup()
  //   .setLngLat(coordinates)
  //   // .setHTML("LAT/LON: " + coordinates + ", X/Y COORD: " + xcoord + ", " ycoord)
  //   .addTo(map);
  // });

  // Change the cursor to a pointer when the mouse is over the places layer.
  map.on('mouseenter', 'points', function () {
    map.getCanvas().style.cursor = 'pointer';
  });

  // Change it back to a pointer when it leaves.
  map.on('mouseleave', 'points', function () {
    map.getCanvas().style.cursor = '';
  });
});

// CONNECT TO ATHENA, QUERY FOR MAP COORDINATES

var athena = new AWS.Athena({
  accessKeyId: "AKIAJDCEWBOHQ2YLZMWA",
  secretAccessKey: "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",
  region: "us-east-1"
});


var sql_results;

var coords_set = new Set();
var sql_length = 0;
count = 0;
features_count = 0;

function submitAthenaQuery(sql_command, instructions) {
  console.log("SUBMITTED ATHENA QUERY")
  var params = {
    QueryString: sql_command,
    ResultConfiguration: {
      EncryptionConfiguration: {
        EncryptionOption: "SSE_S3"
      },
      OutputLocation: 's3://aws-athena-query-results-pluto-us-east-1/'
    }
  };

  response = athena.startQueryExecution(params, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else {
      var q_id = data['QueryExecutionId'];
      setTimeout( function() {
        waitForQueryToComplete(q_id, instructions);
      }, 3000);

    }
  });
}

function waitForQueryToComplete(q_id, instructions) {
  console.log("WAITING FOR QUERY TO COMPLETE: " + q_id + ", for " + instructions)
  athena.getQueryExecution({QueryExecutionId: q_id}, function(err, data) {
    if (err) console.log(err, err.stack);
    else  {
      var isRunning = true;
      while (isRunning) {
        // if (data["QueryExecution"]["Status"]["State"] == "RUNNING") {}
        if (data["QueryExecution"]["Status"]["State"] == "SUCCEEDED") {
          isRunning = false;
          params = {QueryExecutionId: q_id};
          processQueryResults(params, instructions);
        }
      }
    }
  });
}

var i = 0;
var next_token = '';
var add_layer_count = 0;

function processQueryResults(params, instructions) {
  var output;
  var geojson_results;
  console.log("PROCESSING QUERY RESULTS")
  athena.getQueryResults(params, function(err, data) {
      if (err) console.log(err, err.stack);
      else {
        next_token = data['NextToken']
        console.log(data)
        if (instructions == "FIND ZONING LABELS") {
          compileZoningDistrictLabels(data);
        } else if (instructions == "ADD LAYER") {
          geojson_results = format_into_geojson_string(data)
        }
      }


      if ( i < 4) {
        i++;
      } else {
        i = 0;
      }
      count++;
      circle_colors = ['#fbb03b', '#223b53', '#e55e5e', '#3bb2d0', '#573bfa']
      if (next_token != undefined && instructions == "ADD LAYER") { // More data to retrieve from getQueryResults
        new_params = {
          NextToken: next_token,
          QueryExecutionId: params.QueryExecutionId
        }
        processQueryResults(new_params, instructions);
        add_layer_count++;
        activeLayersOnMap.push(count.toString() + 'MapID')
        map.addLayer({
          "id": count.toString() + 'MapID',
          "type": 'circle',
          "source": {
            "type": 'geojson',
            "data": geojson_results
          },
          "paint": {
            'circle-color': circle_colors[i]
          }
        });

      } else if (next_token == undefined && instructions == "ADD LAYER") { // Currently on last piece of data
        activeLayersOnMap.push(count.toString() + 'MapID')
        map.addLayer({
          "id": count.toString() + 'MapID',
          "type": 'circle',
          "source": {
            "type": 'geojson',
            "data": geojson_results
          },
          "paint": {
            'circle-color': circle_colors[i]
          }
        });

        console.log("# OF FEATURES ADDED: " + features_count)
      }
  });
  return output;
}


function format_into_geojson_string(sql_results) {
  features_count = 0;
  var geojson_query = {'type': 'FeatureCollection', 'features':[]}

  sql_length += sql_results["ResultSet"]["Rows"].length;
  for (var i=1; i < sql_results["ResultSet"]["Rows"].length; i++) { // start at 1 because first row is for headers
    features_count ++;
    long = sql_results["ResultSet"]["Rows"][i]["Data"][1]["VarCharValue"]
    lat = sql_results["ResultSet"]["Rows"][i]["Data"][0]["VarCharValue"]

    coords_set.add([long, lat])

    feature = {'type': 'Feature',
                     'properties': {},
                     'geometry': {'type': 'Point',
                                  'coordinates': [parseFloat(long), parseFloat(lat)] }}

    geojson_query['features'].push(feature)
  }
  return geojson_query;
}

var allFeaturesToApply = {} // {'blgarea': 2000, 'assessTot': 45}

function createNewQueryString(featureToValue) {

  queryFeatureString = '';
  queryValueString = 'WHERE ';
  for (var key in featureToValue) {
    queryFeatureString += ', ' + key;

    if (queryValueString != 'WHERE ') {
      queryValueString += ' and'
    }
    if (key == "zonedist1") {
      queryValueString += ' zonedist1=' + '\'' + featureToValue[key].toUpperCase() + '\'';
    } else if (key == "landuse") {
      for (let item of featureToValue[key]) {
        var sentence = queryValueString.split(" ");
        console.log("sentence: " + sentence)
        if (queryValueString != 'WHERE ' && sentence[sentence.length - 1] != 'and') {
          queryValueString += ' or '
        }
        queryValueString += ' landuse=' + '\'' + item.toUpperCase() + '\'';
      }
    } else { // for values on slider
      min = featureToValue[key][0]
      max = featureToValue[key][1]
      // string looks like - 'WHERE blgarea > 0 and blgarea <= 2000' (where 2000 is dynamically determined by user)
      queryValueString += ' ' + key + '>=' + min.toString() + ' and ' + key + '<=' + max.toString();
    }

  }

  sql_command = 'SELECT lat, long' + queryFeatureString + ' FROM "pluto_db"."pluto_latlonincluded" ' + queryValueString + ' limit 1500;'
  console.log("sql_command is: " + sql_command)
  return sql_command;
}

function refreshMap() {
  for (var layer in activeLayersOnMap) {
    var mapLayer = map.getLayer(activeLayersOnMap[layer]);
    if (typeof(mapLayer) != undefined) {
      map.removeLayer(activeLayersOnMap[layer]);
    }
  }
  activeLayersOnMap = []
}

$("#slider-yearBuilt").ionRangeSlider({
  onFinish: function(data) {
    min = data.from;
    max = data.to;
    console.log("YEAR BUILT UPDATED")
    allFeaturesToApply['yearBuilt'] = [min, max]
    queryString = createNewQueryString(allFeaturesToApply);
    refreshMap();
    submitAthenaQuery(queryString, "ADD LAYER");
  }
});

$("#slider-bldgarea").ionRangeSlider( {
  onFinish: function(data) {
    console.log("BUILDING AREA UPDATED")
    allFeaturesToApply['blgarea'] = [data.from, data.to]
    queryString = createNewQueryString(allFeaturesToApply);
    refreshMap();
    submitAthenaQuery(queryString, "ADD LAYER");
  }
});

$("#slider-assesTot").ionRangeSlider({
  onFinish: function(data) {
    console.log("BUILDING AREA UPDATED")
    allFeaturesToApply['assesTot'] = [data.from, data.to]
    queryString = createNewQueryString(allFeaturesToApply);
    refreshMap();
    submitAthenaQuery(queryString, "ADD LAYER");
  }
});

$("#slider-landuse").ionRangeSlider({
  onFinish: function(data) {
    console.log("LAND USE UPDATED")
    allFeaturesToApply['landuse'] = [data.from, data.to]
    queryString = createNewQueryString(allFeaturesToApply);
    refreshMap();
    submitAthenaQuery(queryString, "ADD LAYER");
  }
});

$('#dropdown-zoneDist').change(function() {
  console.log("ZONE DIST MENU UPDATED")
  var selectedZoneDist = $('#dropdown-zoneDist-input').val();
  if (selectedZoneDist != undefined) {
    allFeaturesToApply["zonedist1"] = selectedZoneDist;
    queryString = createNewQueryString(allFeaturesToApply);
    refreshMap();
    submitAthenaQuery(queryString, "ADD LAYER");
  }
})

$(".landUseButton").click(function() {
  console.log("LAND USE UPDATED")
  if ($(this).attr('class') == "landUseButton btn btn-outline-primary waves-effect") {
    this.className = "landUseButton btn btn-primary waves-effect"
  } else {
    this.className = "landUseButton btn btn-outline-primary waves-effect"
  }

  var x = document.getElementsByClassName("landUseButton btn btn-primary waves-effect");
  var activeLandUse = []
  for (let item of x) {
    activeLandUse.push(item.innerHTML + '.0')
  }

  allFeaturesToApply['landuse'] = activeLandUse
  queryString = createNewQueryString(allFeaturesToApply);
  refreshMap();
  submitAthenaQuery(queryString, "ADD LAYER")

})
