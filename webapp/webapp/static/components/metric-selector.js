var MetricSelector = React.createClass({
	choices: [
			"precision",
			"recall",
			"auc",
			"f1",
			"true positives",
			"true negatives",
			"false positives",
			"false negatives"
	],
	inputName: function() {
		return "parameter" + this.props.index.toString();
	},
	selectName: function() {
		return "metric" + this.props.index.toString();
	},
	handleDelete: function() {
		this.props.handleDeleteClick(this.props.index);
	},
	render: function() {
		return (
			<div>
				<select name={this.selectName()}>
				{this.choices.map(function(choice){
					return <option value={choice}>{choice}</option>;
				})}
				</select>
				<span style={ { margin: '0 1em 0 1em' }}>@</span>
				<input type='text' name={this.inputName()} size="3" /> %
				<button
					className='btn btn-xs btn-danger'
					type='button'
					onClick={this.handleDelete}>
					X
				</button>
			</div>
		);
	}
});
