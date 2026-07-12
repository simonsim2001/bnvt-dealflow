#!/usr/bin/env python3
import re
import subprocess
import tempfile
import sys

def check_syntax(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        code = f.read()

    # 1. Strip out JSX comments: {/* ... */}
    code = re.sub(r'\{\/\*.*?\*\/\s*\}', '', code, flags=re.DOTALL)

    # 2. Strip out JSX self-closing tags: <Tag attrs />
    # We do this multiple times to handle nested tags if any
    for _ in range(5):
        code = re.sub(r'<[A-Za-z0-9_.]+(?:\s+[A-Za-z0-9_-]+(?:=(?:"[^"]*"|\'[^\']*\'|\{[^{}]*\}))?)*\s*\/>', 'null', code)

    # 3. Strip out JSX open and close tags: <Tag attrs> and </Tag>
    # To avoid matching standard comparison operators (like a < b), we match tags
    # that start with a capital letter or standard tag names, followed by attributes or closing
    code = re.sub(r'<\/?[A-Za-z0-9_.]+(?:\s+[A-Za-z0-9_-]+(?:=(?:"[^"]*"|\'[^\']*\'|\{[^{}]*\}))?)*\s*>', ' ', code)

    # Write to a temporary file
    with tempfile.NamedTemporaryFile(suffix='.js', mode='w+', encoding='utf-8', delete=False) as tmp:
        tmp.write(code)
        tmp_name = tmp.name

    # Run macOS JavaScriptCore compiler check
    jsc_path = '/System/Library/Frameworks/JavaScriptCore.framework/Versions/A/Resources/jsc'
    try:
        res = subprocess.run([jsc_path, '-check', tmp_name], capture_output=True, text=True)
        if res.returncode == 0:
            print("Success: JavaScriptCore syntax check passed!")
            return True
        else:
            print("Syntax Error detected by JavaScriptCore:")
            # Map the temp file line numbers back to original file if possible, or just print output
            print(res.stderr)
            return False
    except Exception as e:
        print(f"Error running jsc: {e}")
        return False

if __name__ == '__main__':
    filename = 'bnvt-dealflow.jsx'
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    check_syntax(filename)
