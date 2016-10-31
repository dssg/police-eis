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
        $("#results-head").empty();
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
                var head = Object.keys(data[0]);
                head.splice(head.indexOf('model_id'),1);
                //console.log(head);
                var results_head = '';
                for (j=0; j< head.length; j++) {
                    results_head = results_head.concat('<th>'+head[j]+'</th>');
                };
                //console.log(results_head)
                $("#results-head").append('<tr><th>&nbsp; &nbsp; &nbsp;</th>' + '<th>Model ID</th>' + results_head + '</tr>')
                // loop through results, append to dom
                for (i = 0; i < data.length; i++) {
                    var url =  flask_util.url_for("get_model_prediction", {model_id:data[i]['model_id']});
                    var columns = '';
                    for (j=0; j< head.length; j++) {
                        columns = columns.concat('<td>'+data[i][head[j]].toPrecision(4)+'</td>');
                    };
                    //console.log(columns)
                        $("#results").append('<tr><td>'+(i+1)
                                          +'</td><td><a href="'
                                          + url
                                          +'" method="post">'
                                          +data[i]['model_id']+'</td>'
                                          +columns + '</tr>'
                                   );
                };
            }
        });
        });
    });


$("body").on("click", ".delete", function (e) {
  $(this).closest("div").remove();
});

var m = 0
var choices = ["precision", "recall", "auc", "f1", "true positives", "true negatives", "false positives", "false negatives"];
function addInput(divName) {
    var input = $("<input/>").attr({type:"text", name:"parameter"+ m.toString(), size:"3"});
    var select = $("<select/>").attr("name","metric"+ m.toString());
    var button = "<button class='btn btn-xs btn-danger delete'>-</button>"
    var div = $("<div>")
    $.each(choices, function(a, b) {
        select.append($("<option/>").attr("value", b).text(b));
    });
    //select.wrap("<div></div>");
    console.log(div.append(select).append(" @ ").append(input).append(" % ").append(button).append("</br>"));
    $("#"+divName).append(div);//append(select).append(" @ ").append(input).append(" % ").append(button).append("</br>");
    console.log($("#"+divName));
    m = m + 1;
}


$(function()
{
    $(document).on('click', '.btn-add', function(e)
    {
        e.preventDefault();

        var controlForm = $('.controls form:first'),
            currentEntry = $(this).parents('.entry:first'),
            newEntry = $(currentEntry.clone()).appendTo(controlForm);

        newEntry.find('input').val('');
        controlForm.find('.entry:not(:last) .btn-add')
            .removeClass('btn-add').addClass('btn-remove')
            .removeClass('btn-success').addClass('btn-danger')
            .html('<span class="glyphicon glyphicon-minus"></span>');
    }).on('click', '.btn-remove', function(e)
    {
    $(this).parents('.entry:first').remove();

    e.preventDefault();
    return false;
  });
});


function addMetric(divName) {
  console.log("addmetric!")
  select.attr("onchange","addMetric('dynamicInput')")
  if ($("#" + divName + " option:selected") == "precision") {
    $("#" + divName).append(" @ ").append(input).append(" % ");
  }
}
