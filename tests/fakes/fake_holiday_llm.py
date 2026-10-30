"""Stand-in for the holiday LLM CLI in tests: prints $FAKE_LLM_VERDICT, exits with $FAKE_LLM_EXIT."""

import os
import sys

sys.stdout.write(os.environ.get("FAKE_LLM_VERDICT", ""))
sys.exit(int(os.environ.get("FAKE_LLM_EXIT", "0")))
