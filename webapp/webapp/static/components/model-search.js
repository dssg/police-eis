var ModelSearcher = React.createClass({
	getInitialState: function() {
		return {
			modelData: [],
			loading: false
		};
	},
	handleSearch: function() {
		var self = this;
		self.setState({ loading: true });
        $.ajax({
            type: "POST",
            url: "/evaluations/search_models",
            data: $("form").serialize(),
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
						<input
							name="timestamp"
							id="timestamp"
							defaultValue="2016-08-03"
							data-format="YYYY-MM-DD"
							data-template="YYYY MMM D" />
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
