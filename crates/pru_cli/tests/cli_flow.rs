use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

fn cli_cmd() -> Command {
    Command::new(assert_cmd::cargo::cargo_bin!("pru_cli"))
}

#[test]
fn add_atoms_and_query() {
    let tmp = tempdir().expect("tempdir");
    let dir = tmp.path().to_str().unwrap();

    cli_cmd().args(["init", "--dir", dir]).assert().success();

    cli_cmd()
        .args(["entity", "add", "--dir", dir, "--name", "Earth"])
        .assert()
        .success();

    cli_cmd()
        .args(["predicate", "add", "--dir", dir, "--name", "orbits"])
        .assert()
        .success();

    cli_cmd()
        .args(["literal", "add", "--dir", dir, "--value", "Sun"])
        .assert()
        .success();

    cli_cmd()
        .args([
            "fact",
            "add",
            "--dir",
            dir,
            "--subject",
            "Earth",
            "--predicate",
            "orbits",
            "--object",
            "Sun",
            "--pretty",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Earth orbits Sun"));

    cli_cmd()
        .args([
            "fact",
            "list",
            "--dir",
            dir,
            "--subject",
            "Earth",
            "--pretty",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Earth orbits Sun"));

    cli_cmd()
        .args([
            "query",
            "--dir",
            dir,
            "--subject",
            "Earth",
            "--predicate",
            "orbits",
            "--pretty",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Earth orbits Sun"));
}
