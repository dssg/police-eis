var dataset = [ 5, 10, 13, 19, 21, 25, 22, 18, 15, 13,
                11, 12, 15, 20, 18, 17, 16, 18, 23, 25 ];
var w=500, h=200;
var barPadding = 1;
svg = d3.select("#testd3")
        .append("svg")
        .attr("width",w)
        .attr("height",h);

var bar = svg.selectAll("rect")
             .data(dataset)
             .enter()
             .append("rect")
             .attr("x", function(d,i) {
                return i * (w / dataset.length)})
             .attr("y", function(d) {
                return h - 4*d;})
             .attr("width", w / dataset.length - barPadding)
             .attr("height", function(d) {
                return d * 4;
             });

/*
    d3.select("#testd3 svg")
    .data(dataset)
    .attr("class", "bar")
    .style("height", function(d) {
        var barHeight = d * 5;
        return barHeight + "px";
    });
*/
