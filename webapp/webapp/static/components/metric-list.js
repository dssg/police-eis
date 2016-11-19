var MetricList = React.createClass({
    getInitialState: function() {
        return {metrics: []};
    },
    addMetric: function() {
        this.setState(
			{'metrics': this.state.metrics.concat([this.state.metrics.length]) }
		);
    },
	removeMetric: function(metricId) {
		var newMetrics = this.state.metrics.filter(function(item) {
			return item !== metricId;
		});
		this.setState({ 'metrics': newMetrics });
	},
    render: function() {
		var self = this;
        return (
            <span>
            <ul>
              {this.state.metrics.map(function(metricId) {
				return <MetricSelector
						key={metricId}
						index={metricId}
						handleDeleteClick={self.removeMetric} />;
              })}
            </ul>
            <button type="button" onClick={this.addMetric}>Add</button>
            </span>
        )
    }
});
