nv.addGraph(function() {
  var chart = nv.models.lineChart()
                .margin({left: 100})  //Adjust chart margins to give the x-axis some breathing room.
                .useInteractiveGuideline(true)  //We want nice looking tooltips and a guideline!
                .x(function(d) { return d[0] })
                .y(function(d) { return d[1] })
                .showLegend(true)       //Show the legend, allowing users to turn on/off line series.
                .showYAxis(true)        //Show the y-axis
                .showXAxis(true)        //Show the x-axis
                .color(d3.scale.category10().range())
  ;

  chart.xAxis     //Chart x-axis settings
      .axisLabel('Feature Value');

  chart.yAxis     //Chart y-axis settings
      .axisLabel('P(X|Y=Label)')
      .tickFormat(d3.format('.02f'));

  //chart.forceY([0.0, 1.0]);
  /* Done setting the chart up? Time to render it!*/
  d3.select('#fdistchart svg')    //Select the <svg> element you want to render the chart in.
      .datum(gammaData(2,7))         //Populate the <svg> element with chart data...
      .call(chart);          //Finally, render the chart!

  //Update the chart when window resizes.
  nv.utils.windowResize(chart.update);
  return chart;
});

function gammaData(groups, points) { //# groups,# points per group
  var data = [],
      random = d3.random.normal(5,2);
  var beta = 2.5;
  for (i = 0; i < groups; i++) {
    data.push({
      key: 'Label ' + i,
      values: []
    });
    var alpha = random();
    for (j = 0; j < points; j+=0.01) {
      data[i].values.push([100*j,gamma_pdf(j,beta,alpha)]);
    }

  }

  return data;
}

function gamma_pdf(x, beta, alpha){
  var gamma = 1;
  for (var i = alpha-1 ; i > 0 ;i --){
    gamma = gamma * i;
  }

  numerator = Math.pow(beta,alpha)*Math.pow(x,alpha-1)*Math.exp(-beta*x);
  denominator = gamma;

  return numerator / denominator;
}
