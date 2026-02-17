package com.spark.test.domain;

public class Employee {
    public int id;
    public String name;
    public String email;
    public int age;
    public String department;
    public String _corrupt_record; // To capture corrupt records if needed

    // Getters and setters
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String get_corrupt_record() {
        return _corrupt_record;
    }

    public void set_corrupt_record(String _corrupt_record) {
        this._corrupt_record = _corrupt_record;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    @Override
    public String toString() {
        return "Employee{id=" + id + ", name='" + name + "', email='" + email +
                "', age=" + age + ", department='" + department + "'}";
    }
}
