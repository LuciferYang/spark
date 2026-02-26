#!/usr/bin/env python3

import sys
import zipfile
import os

def get_jar_contents(jar_path):
    contents = {}
    try:
        with zipfile.ZipFile(jar_path, 'r') as jar:
            for info in jar.infolist():
                # Normalize path separators
                name = info.filename.replace('\\', '/')
                
                # Ignore directory entries
                if name.endswith('/'):
                    continue
                
                # Ignore build-tool specific metadata
                if name.startswith('META-INF/maven/'):
                    continue
                if name == 'META-INF/MANIFEST.MF':
                    continue
                if name.endswith('.RSA') or name.endswith('.SF') or name.endswith('.DSA'):
                    continue
                if name == 'META-INF/DEPENDENCIES':
                    continue
                if name == 'META-INF/git.properties':
                    continue
                if name == 'META-INF/LICENSE': # Often different due to shading
                     continue
                if name == 'META-INF/NOTICE': # Often different
                     continue
                if name.startswith('META-INF/services/'): # Should check, but might differ in order/comments
                     pass 

                contents[name] = info
    except Exception as e:
        print(f"Error reading {jar_path}: {e}")
        sys.exit(1)
    return contents

def compare_jars(jar1_path, jar2_path):
    print(f"Comparing:\n  Maven: {jar1_path}\n  SBT:   {jar2_path}")
    
    jar1_contents = get_jar_contents(jar1_path)
    jar2_contents = get_jar_contents(jar2_path)
    
    jar1_files = set(jar1_contents.keys())
    jar2_files = set(jar2_contents.keys())
    
    only_in_jar1 = jar1_files - jar2_files
    only_in_jar2 = jar2_files - jar1_files
    common_files = jar1_files & jar2_files
    
    if only_in_jar1:
        print(f"\nOnly in Maven JAR ({len(only_in_jar1)} files):")
        for f in sorted(list(only_in_jar1))[:20]:
            print(f"  {f}")
        if len(only_in_jar1) > 20:
            print(f"  ... and {len(only_in_jar1) - 20} more")
            
    if only_in_jar2:
        print(f"\nOnly in SBT JAR ({len(only_in_jar2)} files):")
        for f in sorted(list(only_in_jar2))[:20]:
            print(f"  {f}")
        if len(only_in_jar2) > 20:
            print(f"  ... and {len(only_in_jar2) - 20} more")

    diff_content = []
    for f in common_files:
        info1 = jar1_contents[f]
        info2 = jar2_contents[f]
        
        # Compare size first
        if info1.file_size != info2.file_size:
            diff_content.append(f"{f} (Size: Maven={info1.file_size}, SBT={info2.file_size})")
        # Compare CRC if size matches (optional, but good for verification)
        elif info1.CRC != info2.CRC:
            # Class files might differ slightly due to compiler timestamps/metadata?
            # Usually .class files should be identical if compiled from same source with same compiler options.
            # But here we are using different build tools, maybe different compiler flags.
            diff_content.append(f"{f} (CRC mismatch)")

    if diff_content:
        print(f"\nContent differences in {len(diff_content)} common files:")
        for f in sorted(diff_content)[:20]:
            print(f"  {f}")
        if len(diff_content) > 20:
            print(f"  ... and {len(diff_content) - 20} more")
            
    if not only_in_jar1 and not only_in_jar2 and not diff_content:
        print("\nSUCCESS: JARs are effectively identical (ignoring excluded metadata).")
        return True
    else:
        print("\nFAILURE: JARs differ.")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: compare_jars.py <jar1> <jar2>")
        sys.exit(1)
    
    compare_jars(sys.argv[1], sys.argv[2])
