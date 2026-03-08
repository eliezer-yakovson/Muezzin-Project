# BDS Analysis Logic
## Scoring method: 
The system scans the text and gives 2 points for serious words and 1 point for regular words.

## Relative calculation (percentage): 
The score is calculated as a percentage of all words in the text. This ensures that a long text will not receive a false alarm just because of one problematic word that was said by chance.

## Alert threshold (6.0): 
The threshold is set to 6 so that the system will only alert when there is a deliberate and consistent use of the threat words, and will filter out accidental mentions.

## Threat classification: 
The division into three risk levels (None, Medium, High) allows the system to automatically sort the results, so that it can immediately focus on the most serious cases.