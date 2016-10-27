$(function(){
    $('#timestamp').combodate({
          minYear: 2016,
          maxYear: moment().format('YYYY')
    });
});

$("#results-table").hide();

/*
$(function() {
  $("#GoButton").click(function() {
    $.ajax({
          type: "POST",
          url: "/evaluations/search_models",
          data: JSON.stringify({"data":$("#form_arg").serializeArray()}),
          contentType: "application/json",
          dataType: 'json',
          success: function(ret) {
              }
            });
        });
});
*/

$(function() {
    $("#GoButton").click(function() {
        $("#results").empty();
        $("#results-table").show();
        console.log("pressed!");
        $.ajax({
            type: "POST",
            url: "/evaluations/search_models",
            data: $("form").serialize(),
            success: function(result) {
                console.log("load data!");
                console.log(result.results);
                var data = result.results;
                // show table
                $("#results-table").show();
                console.log(data);
                // loop through results, append to dom
                for (i = 0; i < data.length; i++) {
                    var url =  flask_util.url_for("get_model_prediction", {model_id:data[i]['model_id']});
                    $("#results").append('<tr><th>'+(i+1)
                                          +'</th><td><a href="'
                                          + url
                                          +'" method="post">'
                                          +data[i]['model_id']+'</td><td>'
                                          +data[i]['precision@5.0']+'</td><td>' + '</tr>'
                                   //       +data[i]['run_time']+'</td><td>'
                                   //       +data[i]['metric']+'</td><td>'
                                   //       +data[i]['parameter']+'</td><td>'
                                   //       +data[i]['value'].toPrecision(4)+'</tr>'
                                   );
                };
            }
        });
        });
    });

var m = 0
var choices = ["precision", "recall", "auc", "f1", "true positives", "true negatives", "false positives", "false negatives"];
function addInput(divName) {
    var input = $("<input/>").attr({type:"text", name:"parameter"+ m.toString(), size:"3"});
    var select = $("<select/>").attr("name","metric"+ m.toString());
    $.each(choices, function(a, b) {
        select.append($("<option/>").attr("value", b).text(b));
    });
    $("#" + divName).append(select).append(" @ ").append(input).append(" % ").append("</br>")
    m = m + 1
}
