import time

from services.orchestrator.graph import run_with_cache

incident = "Payment API failing with 500 errors on /api/payment/process"

print("\n" + "="*60)
print("🔄 First run (cache miss — runs full graph)...")
print("="*60)
t1 = time.time()
result1 = run_with_cache(incident)
t2 = time.time()

for analysis in result1.get("analyses", []):
    print(f"\n🤖 Agent: {analysis.agent_name}")
    print(f"   Confidence: {analysis.confidence:.0%}")
    print("   Findings:")
    for f in analysis.findings:
        print(f"     • {f}")

rca = result1["rca_report"]
print("\n" + "="*60)
print("📋 FINAL RCA REPORT")
print("="*60)
print(f"\n🎯 Probable Cause: {rca.get('probable_cause')}")
print(f"📊 Confidence:     {rca.get('confidence', 0):.0%}")
print("🔧 Remediation Steps:")
for step in rca.get("remediation", []):
    print(f"   • {step}")

print(f"\n⏱  Time taken : {t2 - t1:.1f}s")
print(f"💾 Cache hit  : {result1.get('cache_hit', False)}")

print("\n" + "="*60)
print("⚡ Second run (should be instant cache hit)...")
print("="*60)
t3 = time.time()
result2 = run_with_cache(incident)
t4 = time.time()

rca2 = result2["rca_report"]
print(f"\n🎯 Probable Cause: {rca2.get('probable_cause')}")
print(f"\n⏱  Time taken : {t4 - t3:.1f}s")
print(f"💾 Cache hit  : {result2.get('cache_hit', False)}")
