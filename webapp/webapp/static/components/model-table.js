var ModelTable = React.createClass({
	linkedData: function() {
		return this.props.data.map(function(row) {
			var url = flask_util.url_for("get_model_prediction", {model_id:row.model_id});
			row.model_id = Reactable.unsafe('<a href="' + url + '" method="post">' + row.model_id + '</a>');
			return row;
		});
	},
	columnOrder: function() {
		if(this.props.data.length > 0) {
			var header = Object.keys(this.props.data[0]);
			header.splice(header.indexOf('model_id'),1);
			header.splice(0, 0, 'model_id');
			return header;
		} else {
			return [''];
		}
	},
	render: function() {
		return (
			<Reactable.Table
				className="table"
				columns={this.columnOrder()}
				data={this.linkedData()}
				pageButtonLimit={5}
				sortable={true}
				itemsPerPage={15} />
		);
	}
});
