var ModelSearcher = React.createClass({
	getInitialState: function() {
		return {
			modelData: [],
			loading: false,
			startDate: moment('2016-08-03'),
		};
	},
	handleSearch: function() {
		var self = this;
		self.setState({ loading: true });
		var params = { timestamp: this.state.startDate.format('YYYY-MM-DD') };
		$.each($('form').serializeArray(), function(_, kv) {
		  params[kv.name] = kv.value;
		});
        $.ajax({
            type: "POST",
            url: "/evaluations/search_models",
            data: $.param(params),
            success: function(result) {
				self.setState({
					modelData: result.results,
					loading: false
				});
			}
		})
	},
	renderLoader: function() {
		return (
			<div id="loader" style={{ margin: "0 auto"}} className="loader"></div>
		);
	},
	renderTable: function() {
		return (
			<ModelTable data={ this.state.modelData } />
		);
	},
	handleDateChange: function(dt) {
		this.setState({startDate: dt});
	},
	render: function() {
		return (
			<div className="container center-container">
				<div className="col-lg-3">
					<form method="post" role="form" id="form_arg">
						<div className="row">
							<MetricList />
						</div>
						<div className="row">
						After
						<DatePicker
							selected={this.state.startDate}
							onChange={this.handleDateChange} />
						&nbsp; &nbsp;
						</div>
						<div className="row">
							<button
								type="button"
								className="btn btn-primary btn-sm"
								onClick={this.handleSearch}>
								Go
							</button>
						</div>
					</form>
				</div>
				<div className="col-lg-9">
					<div className="row">
						<div className="col-lg-12">
							{ this.state.loading ? this.renderLoader() : this.renderTable() }
						</div>
					</div>
				</div>
			</div>
		);
	}
});
