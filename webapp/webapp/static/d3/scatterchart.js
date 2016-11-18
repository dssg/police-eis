
nv.addGraph(function() {
  var chart = nv.models.scatterChart()
                .showDistX(true)    //showDist, when true, will display those little distribution lines on the axis.
                .showDistY(true)
                .useVoronoi(true)
                .color(d3.scale.category10().range())
                .duration(300);

  //Configure how the tooltip looks.

  chart.dispatch.on('renderEnd', function(){
            console.log('render complete');
        });

  //Axis settings
  chart.xAxis.tickFormat(d3.format('.02f'));
  chart.yAxis.tickFormat(d3.format('.02f'));

  var myData = randomData(2,40);
  d3.select('#scatter svg')
      .datum(myData)
      .call(chart);

  nv.utils.windowResize(chart.update);
  chart.dispatch.on('stateChange', function(e) { ('New State:', JSON.stringify(e)); });
  return chart;
});




/**************************************
 * Simple test data generator
 */
function randomData(groups, points) { //# groups,# points per group
  var data = [],
      random = d3.random.normal();

  for (i = 0; i < groups; i++) {
    data.push({
      key: 'Label ' + i,
      values: []
    });

    for (j = 0; j < points; j++) {
      data[i].values.push({
        x: random(),
        y: random(),
        size: Math.round(Math.random() * 100) / 100,   //Configure the size of each scatter point
        shape: "circle"  //Configure the shape of each scatter point.
      });
    }
  }

  return data;
}
