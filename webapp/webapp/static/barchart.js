d3.json('/static/fakeFeatureImportanceData.json', function(data) {
nv.addGraph(function() {
    var chart = nv.models.multiBarHorizontalChart()
        .x(function(d) { return d.label })
        .y(function(d) { return d.value })
        .margin({top: 30, right: 20, bottom: 50, left: 175})
        .showValues(true)           //Show bar value next to each bar.
        .showControls(true);        //Allow user to switch between "Grouped" and "Stacked" mode.

    chart.tooltip.enabled()
    chart.yAxis
        .tickFormat(d3.format(',.2f'));

    d3.select('#barchart svg')
        .datum(data)
        .call(chart);

    nv.utils.windowResize(chart.update);

    return chart;
  });
});
