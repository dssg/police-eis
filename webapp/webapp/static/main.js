var data = [];
$("#results-table").hide();

$(function() {
    //
    console.log("ready!");
    $("#GoButton").click(function() {
        $("#results").empty();
        $("#results-table").hide();
        $("#results-table").show();
        console.log("pressed!");
        $.ajax({
            type: "POST",
            url: "/evaluations/search_best_models",
            data: $("form").serialize(),
            success: function(result) {
                console.log("load data!");
                console.log(result.results);
                var data = result.results;
                // show table
                $("#results-table").show();
                console.log(data.length);
                // loop through results, append to dom
                for (i = 0; i < data.length; i++) {
                    $("#results").append('<tr><th><a href="'+'/evaluations/individual">'
                                          +data[i]['run_time']+'</a></th><th>'
                                          +data[i]['model_type']+'</th><th>'
                                          +data[i]['metric']+'</th><th>'
                                          +data[i]['parameter']+'</th><th>'
                                          +data[i]['value']+'</tr>')
                };

            }
        });
        });
    });

