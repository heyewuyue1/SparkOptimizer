import subprocess
import json
from utils.custom_logging import logger

def call_rewriter(db_id, sql_input, rule_input):
    # Provide a list of strings as input
    input_list = [db_id, sql_input, rule_input]
    # Convert the input list to a JSON string
    input_string = json.dumps(input_list)
    command = 'java -cp rewriter_java.jar src/rule_rewriter.java'

    process = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    # process.stdin.write(.encode())
    # Wait for the subprocess to finish and capture the output
    output, error = process.communicate(input=input_string)

    # Print the output and error messages
    # print("Output:\n", output)
    # print("Error:\n", error)
    rew = output.replace("\u001B[32m", '').replace("\u001B[0m", '')
    output = output.replace("\u001B[32m", '').replace("\u001B[0m", '').split('\n')
    ind = 0
    for i in output:
        if not i.startswith('SELECT') and not i.startswith('select') and not i.startswith('with ') and not i.startswith('WITH '):
            pass
        else:
            ind = output.index(i)
            break
    if output[ind-1] == 'No changed!':
        return sql_input
    logger.info(f'raw output: {rew}')
    
    queries = output[ind:-3]
    # print(' '.join(queries))
    output = ' '.join(queries).replace('"', '')
    if 'select' in output or 'SELECT' in output or 'Select' in output:
        # change the functions edited to fit calcite back to original ones
        output = output.replace('SUBSTRING', 'SUBSTR')
        return output
    else:
        logger.error(f'Input: {sql_input}')
        logger.error(f"Output: {output}")
        logger.error(f"Error: {error}")
        return 'NA'



# def create_nested_tree(heights, nodes, filt_meta):
#     if len(heights) <= 3:
#         if filt_meta:
#             return [i[:i.index('(')] for i in nodes]
#         else:
#             return nodes
#     else:
#         if filt_meta:
#             root = nodes[0][:nodes[0].index('(')]
#         else:
#             root = nodes[0]
#         root_h = heights[0]
#         direct_subs = [i for i, x in enumerate(heights) if x == root_h+1]
#         if len(direct_subs) == 2:
#             left_root = [i for i, x in enumerate(heights) if x == root_h+1][0]
#             right_root = [i for i, x in enumerate(heights) if x == root_h+1][1]
#             left_subtree = create_nested_tree(heights[left_root:right_root], nodes[left_root:right_root])
#             right_subtree = create_nested_tree(heights[right_root:], nodes[right_root:])
#             return [root, left_subtree, right_subtree]
#         else:
#             left_root = [i for i, x in enumerate(heights) if x == root_h+1][0]
#             left_subtree = create_nested_tree(heights[left_root:], nodes[left_root:])
#             return [root, left_subtree]

if __name__ == '__main__':
    print(call_rewriter(db_id, sql_input, rule_input))

