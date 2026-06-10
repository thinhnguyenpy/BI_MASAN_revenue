db = db.getSiblingDB('production_db');

db.createCollection("Categories");
db.createCollection("Products");
db.createCollection("Departments");
db.createCollection("ProductionLogs");
db.createCollection("LogisticsCosts");

db.Departments.insertMany([
    { DepartmentID: 1, DepartmentName: "Phân xưởng Sữa nước" },
    { DepartmentID: 2, DepartmentName: "Phân xưởng Sữa chua" }
]);

print("MongoDB initialization completed!");