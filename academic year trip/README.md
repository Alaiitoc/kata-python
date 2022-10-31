# Evaluations Kata

## Questions:
Academic year 2019-2020 :
- For each student, find every subjects on which he has been evaluated.
- For each student, calculate his average in each subject.
- Calculate the average of each class in each subject.
- For each written evaluations, find all the students that missed the evaluation.
- For each class and for each oral evaluations, find the upper and lower bounds for that evaluation period for the whole class.
- Create an object representing a class with relevant field/members.

If some points are unclear feel free to take your own decision and justify in comments.

## Data structure example :
```
{
	"teachers": {
		"teacher-id-1" : {
			"name": "",
			"surname": "",
			"classes": ["class-id-1", "class-id-2", ...],
			"subject": "subject-id-2"
		},
		...
	},
	"students": {
		"student-id-1": {
			"name": "",
			"surname": "",
			"class": "class-id-1"
		},
		...
	},
	"evaluations": [
		{
			"student": "student-id-1",
			"subject": "subject-id-1",
			"score": 5,
			"date": "ISO with timestamp",
			"type": "written/oral"
		},
		...
	]
}
```

## Crédits
challenge original Société Générale Investment Banking
