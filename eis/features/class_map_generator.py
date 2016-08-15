with open('class_names.txt') as file:
	for line in file:
		print("\'" + line.strip() + "\': dispatches." + line.strip() + "(**kwargs),")