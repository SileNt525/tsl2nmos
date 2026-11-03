package tally

import (
	"testing"
	"time"

	"tsl2nmos/internal/config"
	"tsl2nmos/internal/tsl"
)

func TestEvaluateRuleModes(t *testing.T) {
	// 1. Default any_color mode + PGMOverridesPVW=true
	r1 := config.TallyRuleConfig{
		Mode:            config.RuleAnyColor,
		PGMOverridesPVW: true,
	}
	red, grn := EvaluateRule(tsl.LampRed, tsl.LampOff, tsl.LampGreen, r1)
	if !red || grn {
		t.Fatalf("expected PGMOverridesPVW to suppress green (want true/false, got %v/%v)", red, grn)
	}

	// Amber triggers both Red and Green when PGMOverridesPVW=false
	r2 := config.TallyRuleConfig{Mode: config.RuleAnyColor, PGMOverridesPVW: false}
	red, grn = EvaluateRule(tsl.LampAmber, tsl.LampOff, tsl.LampOff, r2)
	if !red || !grn {
		t.Fatalf("expected Amber to activate both red & green, got %v/%v", red, grn)
	}

	// 2. Classic dual-lamp RH=PGM, LH=PVW even when switcher emits Red(1) on LH for Tally2!
	r3 := config.TallyRuleConfig{Mode: config.RuleRHPgmLHPvw}
	red, grn = EvaluateRule(tsl.LampOff, tsl.LampOff, tsl.LampRed, r3)
	if red || !grn {
		t.Fatalf("rh_pgm_lh_pvw failed: want false/true, got %v/%v", red, grn)
	}

	// 3. Polarity inversion
	r4 := config.TallyRuleConfig{Mode: config.RuleAnyColor, InvertRed: true}
	red, grn = EvaluateRule(tsl.LampOff, tsl.LampOff, tsl.LampOff, r4)
	if !red || grn {
		t.Fatalf("InvertRed idle failed: want true/false, got %v/%v", red, grn)
	}

	// 4. SimplyLive ViBox sets TextTally=RED whenever any tally (including PVW Green on RH) is active
	// to illuminate UMD text characters; any_color must NOT false-trigger Red Tally on PVW!
	r5 := config.TallyRuleConfig{Mode: config.RuleAnyColor}
	red, grn = EvaluateRule(tsl.LampGreen, tsl.LampRed, tsl.LampOff, r5)
	if red || !grn {
		t.Fatalf("SimplyLive PVW (RH=Green, Text=Red, LH=Off) failed: want false/true, got %v/%v", red, grn)
	}
	red, grn = EvaluateRule(tsl.LampGreen, tsl.LampRed, tsl.LampRed, r5)
	if !red || !grn {
		t.Fatalf("SimplyLive PGM+PVW (RH=Green, Text=Red, LH=Red) failed: want true/true, got %v/%v", red, grn)
	}
}

func TestEngineDeduplicationOverridesAndTimeout(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.TSL.TimeoutMs = 100
	cfg.TSL.TimeoutAction = "clear"
	cfg.Channels = []config.ChannelConfig{
		{ID: 1, Name: "CAM 1", MatchAllScr: true, Index: 1},
	}

	var events []StateChangeEvent
	eng := NewEngine(cfg, nil)
	eng.OnChange(func(ev StateChangeEvent) {
		events = append(events, ev)
	})

	pkt := tsl.Packet{
		Screen: 0,
		Messages: []tsl.DMSG{
			{Index: 1, RHTally: tsl.LampRed, LHTally: tsl.LampOff, Text: "CAM 1", HasText: true},
		},
	}
	now := time.Now()
	eng.HandlePacket(pkt, "127.0.0.1:9999", now)
	// Duplicate packet must be deduplicated!
	eng.HandlePacket(pkt, "127.0.0.1:9999", now.Add(5*time.Millisecond))

	if len(events) != 1 || events[0].Color != ColorRed || !events[0].Value {
		t.Fatalf("expected exactly 1 Red=true transition after duplicate packet, got %+v", events)
	}

	// Manual override to force_green must turn Red->false and Green->true
	eng.SetOverride(1, OverrideForceGreen)
	if len(events) != 3 {
		t.Fatalf("expected 2 more transitions on force_green, total=%d (%+v)", len(events), events)
	}
	rVal, _ := eng.GetChannelValue(1, ColorRed)
	gVal, _ := eng.GetChannelValue(1, ColorGreen)
	if rVal || !gVal {
		t.Fatalf("unexpected override values: red=%v green=%v", rVal, gVal)
	}

	// Restore auto -> returns to live raw state (Red=true, Green=false)
	eng.SetOverride(1, OverrideAuto)
	rVal, _ = eng.GetChannelValue(1, ColorRed)
	gVal, _ = eng.GetChannelValue(1, ColorGreen)
	if !rVal || gVal {
		t.Fatalf("expected restore auto to resume Red=true/Green=false, got %v/%v", rVal, gVal)
	}

	// Simulate TSL timeout failsafe (>100ms elapsed)
	eng.checkTimeout(now.Add(250 * time.Millisecond))
	rVal, _ = eng.GetChannelValue(1, ColorRed)
	if rVal {
		t.Fatalf("expected timeout clear failsafe to turn Red=false")
	}
}
