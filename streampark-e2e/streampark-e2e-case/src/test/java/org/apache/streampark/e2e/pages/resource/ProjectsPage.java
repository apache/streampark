/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.e2e.pages.resource;

import org.apache.streampark.e2e.pages.common.Constants;
import org.apache.streampark.e2e.pages.common.NavBarPage;

import lombok.Getter;
import lombok.SneakyThrows;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.util.List;

import static org.apache.streampark.e2e.pages.common.CommonFactory.WebElementClick;
import static org.apache.streampark.e2e.pages.common.CommonFactory.WebElementDeleteAndInput;

@Getter
public class ProjectsPage extends NavBarPage implements ResourcePage.Tab {

    @FindBy(id = "e2e-project-create-btn")
    public WebElement buttonCreateProject;

    @FindBy(className = "ant-table-tbody")
    public List<WebElement> projectList;

    @FindBy(className = "e2e-project-build-btn")
    public WebElement buildButton;

    @FindBy(className = "e2e-project-delete-confirm")
    public WebElement deleteConfirmButton;

    @FindBy(className = "e2e-project-build-confirm")
    public WebElement buildConfirmButton;

    public CreateProjectForm createProjectForm;

    public ProjectsPage(RemoteWebDriver driver) {
        super(driver);
        createProjectForm = new CreateProjectForm();
    }

    @SneakyThrows
    public ProjectsPage createProject(String projectName,
                                      String projectUrl,
                                      String projectRefs,
                                      String projectBuildArgument,
                                      String projectDescription) {
        waitForPageLoading();

        WebElementClick(driver, buttonCreateProject);

        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(ExpectedConditions.urlContains("/project/add"));

        createProjectForm = new CreateProjectForm();
        createProjectForm.inputProjectName.sendKeys(projectName);

        createProjectForm.selectCveDropdown.click();

        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(d -> createProjectForm.selectCve.stream().anyMatch(WebElement::isDisplayed));

        createProjectForm.selectCve.stream()
            .filter(e -> e.getText().equalsIgnoreCase("GitHub/GitLab"))
            .findFirst()
            .orElseThrow(() -> new Exception("Cvs not found"))
            .click();

        createProjectForm.inputProjectUrl.sendKeys(projectUrl);
        createProjectForm.selectBranchDropdown.click();

        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(d -> createProjectForm.selectRefs.stream().anyMatch(WebElement::isDisplayed));
        createProjectForm.selectRefs.stream()
            .filter(e -> e.getText().equalsIgnoreCase(projectRefs))
            .findFirst()
            .orElseThrow(() -> new Exception(String.format("Branch not found: %s", projectRefs)))
            .click();

        createProjectForm.inputBuildArgument.sendKeys(projectBuildArgument);
        createProjectForm.inputDescription.sendKeys(projectDescription);
        createProjectForm.buttonSubmit.click();

        waitForListPageAfterSubmit();
        return this;
    }

    @SneakyThrows
    public ProjectsPage editProject(String projectName,
                                    String newProjectName) {
        waitForPageLoading();

        WebElement editButton = projectList.stream()
            .filter(it -> it.getText().contains(projectName))
            .flatMap(
                it -> it.findElements(
                    By.className("e2e-project-edit-btn"))
                    .stream())
            .filter(WebElement::isDisplayed)
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No edit button in project list"));
        WebElementClick(driver, editButton);

        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(ExpectedConditions.urlContains("/project/edit"));

        createProjectForm = new CreateProjectForm();
        WebElementDeleteAndInput(driver, createProjectForm.inputProjectName, newProjectName);
        createProjectForm.buttonSubmit.click();

        waitForListPageAfterSubmit();
        return this;
    }

    @SneakyThrows
    public ProjectsPage buildProject(String projectName) {
        waitForPageLoading();

        WebElement buildBtn = projectList.stream()
            .filter(it -> it.getText().contains(projectName))
            .flatMap(
                it -> it.findElements(By.className("e2e-project-build-btn")).stream())
            .filter(WebElement::isDisplayed)
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No build button in project list"));
        WebElementClick(driver, buildBtn);

        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(ExpectedConditions.elementToBeClickable(buildConfirmButton));

        buildConfirmButton.click();

        return this;
    }

    @SneakyThrows
    public ProjectsPage deleteProject(String projectName) {
        waitForPageLoading();
        WebElement deleteBtn = projectList.stream()
            .filter(it -> it.getText().contains(projectName))
            .flatMap(
                it -> it
                    .findElements(By.className("e2e-project-delete-btn"))
                    .stream())
            .filter(WebElement::isDisplayed)
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No delete button in project list"));
        WebElementClick(driver, deleteBtn);

        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(ExpectedConditions.elementToBeClickable(deleteConfirmButton));

        deleteConfirmButton.click();
        PageFactory.initElements(driver, this);
        return this;
    }

    private void waitForPageLoading() {
        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(d -> isProjectListPage(d.getCurrentUrl()));
        PageFactory.initElements(driver, this);
        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(ExpectedConditions.elementToBeClickable(buttonCreateProject));
        createProjectForm = new CreateProjectForm();
    }

    private void waitForListPageAfterSubmit() {
        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(d -> isProjectListPage(d.getCurrentUrl()));
        PageFactory.initElements(driver, this);
        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(ExpectedConditions.elementToBeClickable(buttonCreateProject));
        createProjectForm = new CreateProjectForm();
    }

    private boolean isProjectListPage(String url) {
        return url.contains("/resource/project")
            || (url.contains("/project") && !url.contains("/project/add") && !url.contains("/project/edit"));
    }

    @Getter
    private class CreateProjectForm {

        CreateProjectForm() {
            PageFactory.initElements(driver, this);
        }

        @FindBy(id = "form_item_name")
        public WebElement inputProjectName;

        @FindBy(xpath = "//div[contains(@codefield, 'repository')]//div[contains(@class, 'ant-select-selector')]")
        public WebElement selectCveDropdown;

        @FindBys({
                @FindBy(css = "[codefield=repository]"),
                @FindBy(className = "ant-select-item-option-content")
        })
        public List<WebElement> selectCve;

        @FindBy(name = "url")
        public WebElement inputProjectUrl;

        @FindBy(xpath = "//div[contains(@codefield, 'refs')]//div[contains(@class, 'ant-select-selector')]")
        public WebElement selectBranchDropdown;

        @FindBys({
                @FindBy(css = "[codefield=refs]"),
                @FindBy(className = "ant-select-item-option-content")
        })
        public List<WebElement> selectRefs;

        @FindBy(id = "form_item_buildArgs")
        public WebElement inputBuildArgument;

        @FindBy(id = "form_item_description")
        public WebElement inputDescription;

        @FindBy(id = "e2e-project-submit-btn")
        public WebElement buttonSubmit;
    }
}
