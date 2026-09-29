"""Runs every day's script one after another and reports pass/fail."""
import pathlib, subprocess, sys

root = pathlib.Path(__file__).parent
failed = []
for script in sorted(root.glob("Day-*/day*.py")):
    print(f"Running {script.parent.name} ...", end=" ", flush=True)
    r = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    print("OK" if r.returncode == 0 else "FAILED")
    if r.returncode:
        failed.append(script.name); print(r.stderr[-800:])
print("\nAll good!" if not failed else f"\nFailed: {failed}")
