name: UAE Market Daily Collector

on:
  schedule:
    - cron: '0 20 * * *'
  workflow_dispatch:
    inputs:
      dry_run:
        description: 'Dry run'
        required: false
        default: 'false'

jobs:
  collect:
    runs-on: ubuntu-latest
    timeout-minutes: 90

    steps:
      - name: Run Apify collector actor
        id: apify_run
        run: |
          RUN_RESPONSE=$(curl -s -X POST \
            "https://api.apify.com/v2/acts/${{ secrets.APIFY_ACTOR_ID }}/runs" \
            -H "Authorization: Bearer ${{ secrets.APIFY_TOKEN }}" \
            -H "Content-Type: application/json" \
            -d '{"memory": 4096, "timeout": 3600}')
          RUN_ID=$(echo "$RUN_RESPONSE" | python3 -c "
          import sys, json
          try:
              d = json.load(sys.stdin)
              print(d['data']['id'])
          except Exception as e:
              print('ERROR: ' + str(e), file=sys.stderr)
              sys.exit(1)
          ")
          echo "Run ID: $RUN_ID"
          echo "run_id=$RUN_ID" >> $GITHUB_OUTPUT
          STATUS="RUNNING"
          # 360 polls × 15s = 90 min max wait
          for i in $(seq 1 360); do
            sleep 15
            POLL=$(curl -s "https://api.apify.com/v2/actor-runs/$RUN_ID" -H "Authorization: Bearer ${{ secrets.APIFY_TOKEN }}")
            STATUS=$(echo "$POLL" | python3 -c "
          import sys, json
          try: d=json.load(sys.stdin); print(d['data']['status'])
          except: print('UNKNOWN')
          ")
            echo "[$i/360] Status: $STATUS"
            if [[ "$STATUS" == "SUCCEEDED" ]]; then echo "Done."; break
            elif [[ "$STATUS" == "FAILED" || "$STATUS" == "ABORTED" || "$STATUS" == "TIMED-OUT" ]]; then
              echo "Failed: $STATUS"; exit 1; fi
          done
          if [[ "$STATUS" != "SUCCEEDED" ]]; then echo "Polling timeout."; exit 1; fi

      - name: Fetch actor output
        id: fetch_output
        run: |
          RUN_ID="${{ steps.apify_run.outputs.run_id }}"
          sleep 5
          OUTPUT=$(curl -s "https://api.apify.com/v2/actor-runs/$RUN_ID/key-value-store/records/OUTPUT" -H "Authorization: Bearer ${{ secrets.APIFY_TOKEN }}")
          echo "$OUTPUT" | python3 -c "
          import sys, json
          raw = sys.stdin.read().strip()
          if not raw:
              print('ERROR: Empty', file=sys.stderr)
              sys.exit(1)
          try:
              d = json.loads(raw)
              print('Keys: ' + str(list(d.keys())))
              errs = d.get('errors', [])
              print('Errors: ' + str(len(errs)))
              for e in errs:
                  print('  FAILED: ' + str(e.get('source')) + ' - ' + str(e.get('error'))[:80], file=sys.stderr)
          except Exception as ex:
              print('ERROR: ' + str(ex), file=sys.stderr)
              sys.exit(1)
          "
          echo "$OUTPUT" > /tmp/apify_output.json
          ERRORS=$(python3 -c "import json; d=json.load(open('/tmp/apify_output.json')); print(len(d.get('errors',[])))")
          # Allow up to 20 errors (24 targets total — only abort if catastrophic)
          if [ "$ERRORS" -gt "20" ]; then echo "Too many failures."; exit 1; fi

      - name: Fetch current Gist history
        run: |
          curl -s "https://gist.githubusercontent.com/imaginationking2/6864f9c206558d36b9777b00f3758087/raw/uae_history.json" -o /tmp/uae_history.json
          echo "Gist: $(wc -c < /tmp/uae_history.json) bytes"

      - name: Merge and build updated history JSON
        run: |
          python3 << 'PYEOF'
          import json
          with open('/tmp/apify_output.json') as f:
              new_data = json.load(f)
          with open('/tmp/uae_history.json') as f:
              history = json.load(f)
          today = new_data['date']
          print('Merging: ' + today)

          def upsert(series, entry, key='date'):
              if entry is None: return series
              series = [e for e in series if e.get(key) != entry.get(key)]
              series.append(entry)
              series.sort(key=lambda e: e.get(key, ''))
              return series

          def last_known(series):
              return series[-1] if series else None

          # Motors (used cars only)
          if new_data.get('dubizzle_motors_entry'):
              history.setdefault('dubizzle_motors_series', [])
              history['dubizzle_motors_series'] = upsert(history['dubizzle_motors_series'], new_data['dubizzle_motors_entry'])
              print('  motors: OK used_cars=' + str(new_data['dubizzle_motors_entry'].get('used_cars')))
          else:
              print('  motors: MISSING')

          # Property for-sale (per-emirate object)
          if new_data.get('dubizzle_property_sale_entry'):
              history.setdefault('dubizzle_property_sale_series', [])
              history['dubizzle_property_sale_series'] = upsert(history['dubizzle_property_sale_series'], new_data['dubizzle_property_sale_entry'])
              e = new_data['dubizzle_property_sale_entry']
              print('  property_sale: OK uae=' + str(e.get('uae')) + ' dubai=' + str(e.get('dubai')) + ' ajman=' + str(e.get('ajman')))
          else:
              print('  property_sale: MISSING')

          # Property for-rent (per-emirate object)
          if new_data.get('dubizzle_property_rent_entry'):
              history.setdefault('dubizzle_property_rent_series', [])
              history['dubizzle_property_rent_series'] = upsert(history['dubizzle_property_rent_series'], new_data['dubizzle_property_rent_entry'])
              e = new_data['dubizzle_property_rent_entry']
              print('  property_rent: OK uae=' + str(e.get('uae')) + ' dubai=' + str(e.get('dubai')) + ' ajman=' + str(e.get('ajman')))
          else:
              print('  property_rent: MISSING')

          # Jobs
          if new_data.get('dubizzle_jobs_entry'):
              history.setdefault('dubizzle_jobs_series', [])
              history['dubizzle_jobs_series'] = upsert(history['dubizzle_jobs_series'], new_data['dubizzle_jobs_entry'])
              print('  jobs: OK full_time=' + str(new_data['dubizzle_jobs_entry'].get('full_time')))
          else:
              prev = last_known(history.get('dubizzle_jobs_series', []))
              print('  jobs: MISSING - carry forward ' + (prev['date'] if prev else 'none'))

          # Bayut
          if new_data.get('bayut_entry'):
              history['bayut_series'] = upsert(history.get('bayut_series', []), new_data['bayut_entry'])
              print('  bayut: OK d9=' + str(new_data['bayut_entry'].get('district9_listings')))
          else:
              prev = last_known(history.get('bayut_series', []))
              print('  bayut: MISSING - carry forward ' + (prev['date'] if prev else 'none'))

          # Luxury
          if new_data.get('luxury_entry'):
              history['luxury_drops_series'] = upsert(history.get('luxury_drops_series', []), new_data['luxury_entry'])
              print('  luxury: OK drops=' + str(new_data['luxury_entry'].get('drop_count')))
          else:
              prev = last_known(history.get('luxury_drops_series', []))
              print('  luxury: MISSING - carry forward ' + (prev['date'] if prev else 'none'))

          # Ajman
          if new_data.get('ajman_entry'):
              history['ajman_property_series'] = upsert(history.get('ajman_property_series', []), new_data['ajman_entry'])
              print('  ajman: OK sale=' + str(new_data['ajman_entry'].get('ajman_for_sale')))
          else:
              prev = last_known(history.get('ajman_property_series', []))
              print('  ajman: MISSING - carry forward ' + (prev['date'] if prev else 'none'))

          # Stress: 4-component using motors used_cars (component A renamed to dubizzle for back-compat)
          motors = new_data.get('dubizzle_motors_entry') or last_known(history.get('dubizzle_motors_series', []))
          lux = new_data.get('luxury_entry') or last_known(history.get('luxury_drops_series', []))
          bay = new_data.get('bayut_entry') or last_known(history.get('bayut_series', []))
          ajm = new_data.get('ajman_entry') or last_known(history.get('ajman_property_series', []))

          used_cars = motors.get('used_cars', 38770) if motors else 38770
          drop_count = lux.get('drop_count', 1542) if lux else 1542
          d9 = bay.get('district9_listings', 31) if bay else 31
          sale = ajm.get('ajman_for_sale', 0) if ajm else 0
          rent = ajm.get('ajman_for_rent', 1) if ajm else 1
          ratio = round(sale / rent, 3) if rent > 0 else 0

          dubizzle_s = min(25, max(0, round(((used_cars - 38770) / 38770) * 100 / 0.4)))
          luxury_s = min(25, round((drop_count - 1542) / 1542 * 100 / 2))
          bayut_s = min(25, round((31 - d9) / 31 * 100 / 2))
          ratio_s = min(25, max(0, round(ratio * 10 - 12)))
          total = dubizzle_s + luxury_s + bayut_s + ratio_s

          band = ('Stable - no signal' if total < 30
              else 'Mild stress building' if total < 45
              else 'Clear stress building' if total < 60
              else 'High stress - monitor closely' if total < 75
              else 'Crisis signal')

          stress_entry = {
              'date': today, 'total': total, 'band': band,
              'components': {'dubizzle': dubizzle_s, 'luxury': luxury_s, 'bayut': bayut_s, 'ajman_ratio': ratio_s},
              'carry_forward': {
                  'dubizzle': new_data.get('dubizzle_motors_entry') is None,
                  'luxury':   new_data.get('luxury_entry') is None,
                  'bayut':    new_data.get('bayut_entry') is None,
                  'ajman':    new_data.get('ajman_entry') is None,
              }
          }

          history['stress_series'] = upsert(history.get('stress_series', []), stress_entry)
          history['last_updated'] = new_data['scraped_at']
          history['latest_stress'] = stress_entry
          history['collection_errors'] = new_data.get('errors', [])

          with open('/tmp/uae_history_updated.json', 'w') as f:
              json.dump(history, f, indent=2)

          cf = [k for k, v in stress_entry['carry_forward'].items() if v]
          print('\n=== STRESS: ' + str(total) + '/100 - ' + band + ' ===')
          print('  A used_cars=' + str(used_cars) + ' score=' + str(dubizzle_s))
          print('  B drops=' + str(drop_count) + ' score=' + str(luxury_s))
          print('  C d9=' + str(d9) + ' score=' + str(bayut_s))
          print('  D ratio=' + str(ratio) + ' score=' + str(ratio_s))
          if cf: print('  * Carried forward: ' + str(cf))
          PYEOF

      - name: Push updated history to Gist
        if: ${{ github.event.inputs.dry_run != 'true' }}
        run: |
          CONTENT=$(python3 -c "
          import json
          with open('/tmp/uae_history_updated.json') as f:
              data = f.read()
          print(json.dumps(data))
          ")
          RESPONSE=$(curl -s -X PATCH \
            "https://api.github.com/gists/6864f9c206558d36b9777b00f3758087" \
            -H "Authorization: Bearer ${{ secrets.GIST_TOKEN }}" \
            -H "Content-Type: application/json" \
            -d "{\"files\": {\"uae_history.json\": {\"content\": $CONTENT}}}")
          echo "$RESPONSE" | python3 -c "
          import sys, json
          d = json.load(sys.stdin)
          print('Gist: ' + str(d.get('html_url','ERROR')) + ' | ' + str(d.get('updated_at','?')))
          "

      - name: Summary
        if: always()
        run: |
          if [ -f /tmp/apify_output.json ]; then
            python3 -c "
          import json
          with open('/tmp/apify_output.json') as f: d = json.load(f)
          errs = d.get('errors', [])
          print('Errors: ' + str([e['source'] for e in errs] if errs else 'none'))
          s = d.get('stress', {})
          print('Stress: ' + str(s.get('total')) + '/100 - ' + str(s.get('band')))
          motors = d.get('dubizzle_motors_entry') or {}
          jobs = d.get('dubizzle_jobs_entry') or {}
          ps = d.get('dubizzle_property_sale_entry') or {}
          pr = d.get('dubizzle_property_rent_entry') or {}
          print('Used cars=' + str(motors.get('used_cars','?')))
          print('Jobs full_time=' + str(jobs.get('full_time','?')))
          print('Sale UAE=' + str(ps.get('uae','?')) + ' Dubai=' + str(ps.get('dubai','?')) + ' Ajman=' + str(ps.get('ajman','?')))
          print('Rent UAE=' + str(pr.get('uae','?')) + ' Dubai=' + str(pr.get('dubai','?')) + ' Ajman=' + str(pr.get('ajman','?')))
          "
          fi
