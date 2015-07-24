// Oh document.ready, how I missed you
$(document).ready(function () {

  $('.dropdown-toggle').dropdown();

  var formChart = function (type) {
    var options = {};
    if (type !== undefined) {
      options.type = type;
    }
    return function (name, label, propertyName, labelName, mappingFunction) {
        $.getJSON('/results/results-' + name.replace('-canvas', '')  + '.json')
          .done(function (json) {
            if (typeof mappingFunction === 'function') {
              json = json.map(mappingFunction);
            }
            var chart = c3.generate({
              bindto: '#' + name + '-graph',
              data: $.extend(options, {
                columns: [
                  [labelName].concat(json.map(function (obj) {
                    return obj[propertyName];
                  }))
                ]
              }),
              axis: {
                x: {
                    type: 'category',
                    categories: json.map(function (obj) {
                      return obj[labelName];
                    })
                }
              }
            });
          });
      };
  };

  var formBarChart = formChart('bar');
  var formAreaChart = formChart('area');

  var mapTimestampsToStrings = function (row) {
    row.range = moment(row.range.split(' - ')[0], 'YYYY-MM-DD').format('MMMM YYYY');
    return row;
  };

  formAreaChart("minor-month", "Unique IPs per month (Minor)", "uniques", "range", mapTimestampsToStrings);
  formAreaChart("periodic-month", "Unique IPs per month (Periodic)", "uniques", "range", mapTimestampsToStrings);
  formBarChart("most-tables", "Most Tables", "num_tables", "num_tables");
  formBarChart("most-servers", "Most Server", "num_servers", "num_servers");
  formBarChart("github-stars", "period", "count", "period");
});
