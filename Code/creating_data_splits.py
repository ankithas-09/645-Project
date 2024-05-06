def split_file(input_file, output_prefix, lines_per_file):
    with open(input_file, 'r') as f:
        lines = f.readlines()

    total_lines = len(lines)
    num_files = total_lines // lines_per_file
    if total_lines % lines_per_file != 0:
        num_files += 1

    for i in range(num_files):
        start_index = i * lines_per_file
        end_index = min((i + 1) * lines_per_file, total_lines)
        output_file = f"{output_prefix}{i}.csv"
        with open(output_file, 'w') as f:
            f.writelines(lines[start_index:end_index])
            # Check if it's the last file and add a new line if necessary
            if i == num_files - 1 and end_index < total_lines:
                f.write('\n')

if __name__ == "__main__":
    input_file = "../Data/processed_data.txt"
    output_prefix = "../Data/adult.split_"
    lines_per_file = 10000
    split_file(input_file, output_prefix, lines_per_file)