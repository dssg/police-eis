class MetricSelector extends React.Component {
	choices() {
		return ["precision", "recall", "auc", "f1", "true positives", "true negatives", "false positives", "false negatives"];
	},
	inputName() {
		return "parameter" + this.props.index.toString();
	},
	selectName() {
		return "metric" + this.props.index.toString();
	},
    render() {
        return (
			<select name={this.selectName()}>@
			{this.choices().map(function(choice){
				return <option value={choice}>{choice}</option>;
			})}
			<input type='text' name={this.inputName()} size=3> %
			<button className='btn btn-xs btn-danger delete'>X</button>
		)
    }
}
