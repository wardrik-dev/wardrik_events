# reach.check runs earlier in the same VM during precommit, and sets the logger back to debug.
Logger.configure(level: :warning)

Mimic.copy(ReqLLM)
Mimic.copy(ReqLLM.StreamResponse)

ExUnit.start()
