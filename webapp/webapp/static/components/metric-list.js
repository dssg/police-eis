class MetricList extends React.Component {
    addMetric() {
        this.metrics.push(this.metrics.length)
    },
    render() {
        return (
            {this.metrics.map(function(metric_num) {
            return <MetricSelector index={metric_num} />
        })}
        <button onClick={this.addMetric}>Add</button>
        )
    }
}
