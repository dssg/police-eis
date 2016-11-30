$(document).ready(function() {
  document.getElementsByTagName("html")[0].style.visibility = "visible";
});



$(function(){
  // **********************************
  //  Description of ALL pager options
  // **********************************
  var pagerOptions = {

    // target the pager markup - see the HTML block below
    container: $(".pager"),

    // use this url format "http:/mydatabase.com?page={page}&size={size}&{sortList:col}"
    ajaxUrl: null,

    // modify the url after all processing has been applied
    customAjaxUrl: function(table, url) { return url; },

    // ajax error callback from $.tablesorter.showError function
    // ajaxError: function( config, xhr, settings, exception ){ return exception; };
    // returning false will abort the error message
    ajaxError: null,

    // add more ajax settings here
    // see http://api.jquery.com/jQuery.ajax/#jQuery-ajax-settings
    ajaxObject: { dataType: 'json' },

    // process ajax so that the data object is returned along with the total number of rows
    ajaxProcessing: null,

    // Set this option to false if your table data is preloaded into the table, but you are still using ajax
    processAjaxOnInit: true,

    // output string - default is '{page}/{totalPages}'
    // possible variables: {size}, {page}, {totalPages}, {filteredPages}, {startRow}, {endRow}, {filteredRows} and {totalRows}
    // also {page:input} & {startRow:input} will add a modifiable input in place of the value
    // In v2.27.7, this can be set as a function
    // output: function(table, pager) { return 'page ' + pager.startRow + ' - ' + pager.endRow; }
    output: '{startRow:input} to {endRow} ({totalRows})',

    // apply disabled classname (cssDisabled option) to the pager arrows when the rows
    // are at either extreme is visible; default is true
    updateArrows: true,

    // starting page of the pager (zero based index)
    page: 0,

    // Number of visible rows - default is 10
    //size: 15,

    // Save pager page & size if the storage script is loaded (requires $.tablesorter.storage in jquery.tablesorter.widgets.js)
    savePages : true,

    // Saves tablesorter paging to custom key if defined.
    // Key parameter name used by the $.tablesorter.storage function.
    // Useful if you have multiple tables defined
    storageKey:'tablesorter-pager',

    // Reset pager to this page after filtering; set to desired page number (zero-based index),
    // or false to not change page at filter start
    pageReset: 0,

    // if true, the table will remain the same height no matter how many records are displayed. The space is made up by an empty
    // table row set to a height to compensate; default is false
    fixedHeight: true,

    // remove rows from the table to speed up the sort of large tables.
    // setting this to false, only hides the non-visible rows; needed if you plan to add/remove rows with the pager enabled.
    removeRows: false,

    // If true, child rows will be counted towards the pager set size
    countChildRows: false,

    // css class names of pager arrows
    cssNext: '.next', // next page arrow
    cssPrev: '.prev', // previous page arrow
    cssFirst: '.first', // go to first page arrow
    cssLast: '.last', // go to last page arrow
    cssGoto: '.gotoPage', // select dropdown to allow choosing a page

    cssPageDisplay: '.pagedisplay', // location of where the "output" is displayed
    cssPageSize: '.pagesize', // page size selector - select dropdown that sets the "size" option

    // class added to arrows when at the extremes (i.e. prev/first arrows are "disabled" when on the first page)
    cssDisabled: 'disabled', // Note there is no period "." in front of this class name
    cssErrorRow: 'tablesorter-errorRow' // ajax error information row

  };

  $("table")
    // Initialize tablesorter
    // ***********************
    .tablesorter({
      theme: 'dropbox',
      widthFixed: true,
      widgets: ['zebra','uitheme']
    })

    // bind to pager events
    // *********************
    .bind('pagerChange pagerComplete pagerInitialized pageMoved', function(e, c){
      var msg = '"</span> event triggered, ' + (e.type === 'pagerChange' ? 'going to' : 'now on') +
        ' page <span class="typ">' + (c.page + 1) + '/' + c.totalPages + '</span>';
      $('#display')
        .append('<li><span class="str">"' + e.type + msg + '</li>')
        .find('li:first').remove();
    })

    // initialize the pager plugin
    // ****************************
    .tablesorterPager(pagerOptions);

});

$( function() {
    $("#tabs").tabs({ activate: function(event, ui) {
        var $activeTab = $("#tabs").tabs('option', 'active');
        console.log($activeTab)
        if ($activeTab != 0 ) {
            $("#pagerp_model").hide();
        } else {$("#pagerp_model").show();}
    }});
  });



/*
$(function() {
    $("#GoBack").click(function() {
        window.location.href = '/';
        var local_results_table = sessionStorage.getItem("local_results_table");
        console.log(JSON.parse(local_results_table));
        //console.log(local_results_table)

    });
});
*/





