/*These lines are all chart setup.  Pick and choose which chart features you want to utilize. */
nv.addGraph(function() {
  var chart = nv.models.lineChart()
                .margin({left: 100})  //Adjust chart margins to give the x-axis some breathing room.
                .useInteractiveGuideline(true)  //We want nice looking tooltips and a guideline!
                .x(function(d) { return d[0].toString() })
                .y(function(d) { return d[1] })
                .showLegend(true)       //Show the legend, allowing users to turn on/off line series.
                .showYAxis(true)        //Show the y-axis
                .showXAxis(true)        //Show the x-axis
  ;

  chart.xAxis     //Chart x-axis settings
      .axisLabel('Year');

  chart.yAxis     //Chart y-axis settings
      .axisLabel('Value')
      .tickFormat(d3.format('.02f'));

  //chart.forceY([0.0, 1.0]);
  /* Done setting the chart up? Time to render it!*/
  var myData = testData();   //You need data...

  d3.select('#testchart svg')    //Select the <svg> element you want to render the chart in.
      .datum(myData)         //Populate the <svg> element with chart data...
      .call(chart);          //Finally, render the chart!

  //Update the chart when window resizes.
  nv.utils.windowResize(chart.update);
  return chart;
});
/**************************************
 * Simple test data generator
 */

function testData() {
  return [
    {
      key: "Base Rate",
      values: [[2011, 0.16], [2012, 0.11], [2013, 0.14], [2014, 0.08], [2015, 0.12], [2016, 0.06]]
    },
    {
      key: "Recall",
      values: [[2011, 0.20], [2012, 0.15], [2013, 0.23], [2014, 0.30], [2015, 0.26], [2016, 0.24]]
    },
    {
      key: "Precision",
      values: [[2011, 0.52], [2012, 0.43], [2013, 0.43], [2014, 0.41], [2015, 0.37], [2016, 0.34]]
    }
  ];
}
