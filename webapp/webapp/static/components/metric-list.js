var MetricList = React.createClass ({
    addMetric: function() {
        this.metrics.push(this.metrics.length)
    },
    render: function() {
        console.log("MetricList checkpoint");
        return (
            {this.metrics.map(function(metric_num) {
                return <MetricSelector index={metric_num} />
            })}
            <button onClick={this.addMetric}>Add</button>
        )
    }
})
