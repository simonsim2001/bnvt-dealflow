#!/usr/bin/env python3
import sys

def check_brackets(filename, start_line=1):
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    
    n = len(content)
    modes = ['js']
    brackets = [] # elements are (char, line, col, idx, from_mode)
    
    line_no = 1
    col_no = 1
    
    i = 0
    pairs = {
        '}': '{',
        ')': '(',
        ']': '['
    }
    
    def is_jsx_tag_start(idx):
        if idx + 1 >= n:
            return False
        next_c = content[idx+1]
        if next_c == '>' or next_c == '/' or next_c == '!':
            return True
        if not next_c.isalpha():
            return False
        j = idx + 1
        while j < n and (content[j].isalnum() or content[j] in ('-', '.', ':')):
            j += 1
        if j < n:
            return content[j].isspace() or content[j] in ('>', '/')
        return False

    def advance(count):
        nonlocal i, line_no, col_no
        for _ in range(count):
            if i >= n:
                break
            if content[i] == '\n':
                line_no += 1
                col_no = 1
            else:
                col_no += 1
            i += 1

    while i < n:
        c = content[i]
        current_line = line_no
        current_col = col_no
        
        current_mode = modes[-1]
        curr_mode_name = current_mode[0] if isinstance(current_mode, tuple) else current_mode
        
        # Handle escape sequence
        if c == '\\' and curr_mode_name in ('single_quote', 'double_quote', 'template', 'regex', 'jsx_attr_single_quote', 'jsx_attr_double_quote'):
            advance(2)
            continue
            
        if curr_mode_name == 'block_comment':
            if c == '*' and i + 1 < n and content[i+1] == '/':
                modes.pop()
                advance(2)
            else:
                advance(1)
            continue
            
        if curr_mode_name == 'line_comment':
            if c == '\n':
                modes.pop()
            advance(1)
            continue
            
        if curr_mode_name == 'single_quote':
            if c == "'":
                modes.pop()
            advance(1)
            continue
            
        if curr_mode_name == 'double_quote':
            if c == '"':
                modes.pop()
            advance(1)
            continue
            
        if curr_mode_name == 'regex':
            if c == '/':
                modes.pop()
            advance(1)
            continue
            
        if curr_mode_name == 'template':
            if c == '`':
                modes.pop()
                advance(1)
            elif c == '$' and i + 1 < n and content[i+1] == '{':
                brackets.append(('${', current_line, current_col, i, 'template'))
                modes.append('js')
                advance(2)
            else:
                advance(1)
            continue
            
        if curr_mode_name == 'jsx_attr_double_quote':
            if c == '"':
                modes.pop()
            advance(1)
            continue
            
        if curr_mode_name == 'jsx_attr_single_quote':
            if c == "'":
                modes.pop()
            advance(1)
            continue
            
        if curr_mode_name == 'jsx_attr_template':
            if c == '`':
                modes.pop()
            advance(1)
            continue
            
        if curr_mode_name == 'js':
            # check comments
            if c == '/' and i + 1 < n and content[i+1] == '*':
                modes.append('block_comment')
                advance(2)
                continue
            elif c == '/' and i + 1 < n and content[i+1] == '/':
                modes.append('line_comment')
                advance(2)
                continue
            # check quotes
            elif c == "'":
                modes.append('single_quote')
                advance(1)
                continue
            elif c == '"':
                modes.append('double_quote')
                advance(1)
                continue
            elif c == '`':
                modes.append('template')
                advance(1)
                continue
            # check regex
            elif c == '/':
                prev_idx = i - 1
                while prev_idx >= 0 and content[prev_idx].isspace():
                    prev_idx -= 1
                prev_char = content[prev_idx] if prev_idx >= 0 else ''
                is_division = prev_char.isalnum() or prev_char in ')]}_$'
                if not is_division:
                    modes.append('regex')
                    advance(1)
                else:
                    advance(1)
                continue
            # check JSX tag start
            elif c == '<':
                if is_jsx_tag_start(i):
                    is_closing = (i + 1 < n and content[i+1] == '/')
                    modes.append(('jsx_tag', is_closing))
                    advance(1)
                    continue
            
            # brackets in js
            if c in '{([':
                brackets.append((c, current_line, current_col, i, 'js'))
            elif c in '})]':
                if not brackets:
                    print(f"Error: Mismatched bracket '{c}' at line {current_line}, col {current_col} (no open brackets)")
                    return False
                top_char, o_line, o_col, o_idx, from_mode = brackets.pop()
                
                if c == '}':
                    if top_char not in ('{', '${'):
                        print(f"Error: Mismatched bracket '{c}' at line {current_line}, col {current_col}. Expected match for '{top_char}' from line {o_line}, col {o_col}")
                        return False
                    if top_char == '${':
                        modes.pop() # returns to 'template'
                    elif from_mode in ('jsx_tag', 'jsx_child_text'):
                        modes.pop() # returns to jsx mode
                else:
                    expected = pairs[c]
                    if top_char != expected:
                        print(f"Error: Mismatched bracket '{c}' at line {current_line}, col {current_col}. Expected match for '{top_char}' from line {o_line}, col {o_col}")
                        return False
            advance(1)
            continue
            
        if curr_mode_name == 'jsx_tag':
            _, is_closing = current_mode
            if c == '"':
                modes.append('jsx_attr_double_quote')
                advance(1)
                continue
            elif c == "'":
                modes.append('jsx_attr_single_quote')
                advance(1)
                continue
            elif c == '`':
                modes.append('jsx_attr_template')
                advance(1)
                continue
            elif c == '{':
                brackets.append(('{', current_line, current_col, i, 'jsx_tag'))
                modes.append('js')
                advance(1)
                continue
            elif c == '>':
                # Check self closing
                back_idx = i - 1
                while back_idx >= 0 and content[back_idx].isspace():
                    back_idx -= 1
                is_self_closing = (back_idx >= 0 and content[back_idx] == '/')
                
                modes.pop() # Pop jsx_tag
                
                if is_closing:
                    # Closing tag: pop 'jsx_element' from brackets
                    if not brackets:
                        print(f"Error: Mismatched closing tag at line {current_line}, col {current_col} (no open JSX elements)")
                        return False
                    top_char, o_line, o_col, o_idx, from_mode = brackets.pop()
                    if top_char != 'jsx_element':
                        print(f"Error: Closed JSX element but expected matching bracket for '{top_char}' from line {o_line}, col {o_col}")
                        return False
                    
                    if modes[-1] == 'jsx_child_text':
                        has_open_jsx = False
                        if brackets:
                            for b in reversed(brackets):
                                if b[0] in ('{', '${', '(', '['):
                                    break
                                if b[0] == 'jsx_element':
                                    has_open_jsx = True
                                    break
                        if not has_open_jsx:
                            modes.pop() # Pop jsx_child_text
                elif is_self_closing:
                    pass
                else:
                    brackets.append(('jsx_element', current_line, current_col, i, 'js'))
                    if modes[-1] != 'jsx_child_text':
                        modes.append('jsx_child_text')
                advance(1)
                continue
            advance(1)
            continue
            
        if curr_mode_name == 'jsx_child_text':
            if c == '{':
                brackets.append(('{', current_line, current_col, i, 'jsx_child_text'))
                modes.append('js')
                advance(1)
                continue
            elif c == '<':
                if is_jsx_tag_start(i):
                    is_closing = (i + 1 < n and content[i+1] == '/')
                    modes.append(('jsx_tag', is_closing))
                    advance(1)
                    continue
            advance(1)
            continue

    if len(modes) > 1:
        print(f"Error: Unclosed modes left: {modes[1:]}")
        return False

    if brackets:
        print(f"Error: {len(brackets)} unclosed brackets left:")
        for top_char, line, col, idx, from_mode in reversed(brackets):
            print(f"  Unclosed '{top_char}' from line {line}, col {col}")
            start_ctx = max(0, idx - 40)
            end_ctx = min(n, idx + 40)
            print(f"  Context: ... {content[start_ctx:idx]} >>> {top_char} <<< {content[idx+1:end_ctx]} ...")
        return False
        
    print("Success: All brackets in the checked section match perfectly!")
    return True

if __name__ == '__main__':
    check_brackets('bnvt-dealflow.jsx', 1)
