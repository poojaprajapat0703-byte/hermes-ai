from services.orchestrator.graph import graph

result = graph.invoke({
    "incident": "Payment API failing with 500 errors on /api/payment/process",
    "classification": {},
    "analyses": [],
    "rca_report": {},
})

print("\n" + "="*60)
print("HERMES ANALYSIS RESULTS")
print("="*60)

for analysis in result["analyses"]:
    print(f"\n🤖 Agent: {analysis.agent_name}")
    print(f"   Confidence: {analysis.confidence:.0%}")
    print("   Findings:")
    for f in analysis.findings:
        print(f"     • {f}")
